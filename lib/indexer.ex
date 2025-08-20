defmodule Kdb.Indexer do
  alias Exqlite.Sqlite3
  alias Kdb.Indexer.Stream
  @filename "indexer.db"

  @spec new(otps :: keyword()) :: reference()
  def new(opts) do
    dbname = Keyword.fetch!(opts, :name)

    case Kdb.Registry.get_db(dbname) do
      nil ->
        folder = Keyword.get(opts, :folder) || raise(ArgumentError, "`folder` is required")
        filename = Path.join(folder, @filename) |> to_charlist()
        conn = open(filename)
        create_tables(conn)
        conn

      kdb ->
        kdb.indexer
    end
  end

  def open(filename) do
    {:ok, conn} = Sqlite3.open(filename, [:readwrite])
    execute(conn, "PRAGMA journal_mode = WAL")
    execute(conn, "PRAGMA synchronous = NORMAL")
    conn
  end

  def close(conn) do
    :ok = Sqlite3.close(conn)
  end

  def destroy(%Kdb{indexer: conn, folder: folder}) do
    drop_tables(conn)
    close(conn)
    File.rm(Path.join(folder, @filename))
  end

  def begin(conn) do
    execute(conn, "BEGIN")
  end

  def commit(conn) do
    execute(conn, "COMMIT")
  end

  @doc """
  Ejecuta una consulta y devuelve el primer resultado.
  """
  def one(conn, sql, params \\ []) do
    case query(conn, sql, params) do
      {:row, []} ->
        {:error, :not_found}

      {:row, row} ->
        {:ok, row}

      error ->
        error
    end
  end

  @doc """
  Verifica si existe al menos un resultado para la consulta dada.
  """
  def exists?(conn, sql, params \\ []) do
    case query(conn, sql, params) do
      {:row, []} -> false
      {:row, _rows} -> true
      _error -> false
    end
  end

  def create_index(conn, cf, field, key, value) do
    sql = "INSERT OR REPLACE INTO secondary_indexes (cf, field, key, value) VALUES (?, ?, ?, ?)"
    execute(conn, sql, [cf, field, key, value])
  end

  def find(conn, cf, field, value, opts \\ []) do
    offset = Keyword.get(opts, :offset, 0)
    limit = Keyword.get(opts, :limit, 100)

    # "SELECT key FROM secondary_indexes WHERE cf = ? AND field = ? AND value LIKE ? COLLATE NOCASE OFFSET ? LIMIT ?"
    sql =
      "SELECT key FROM secondary_indexes WHERE cf = ? AND field = ? AND value LIKE ? OFFSET ? LIMIT ?"

    Stream.query(conn, sql, [cf, field, value, offset, limit])
  end

  def delete_index(conn, cf, key) do
    sql = "DELETE FROM secondary_indexes WHERE cf = ? AND key = ?"
    execute(conn, sql, [cf, key])
  end

  def delete_index(conn, cf, key, field) do
    sql = "DELETE FROM secondary_indexes WHERE cf = ? AND key = ? AND field = ?"
    execute(conn, sql, [cf, key, field])
  end

  defp execute(conn, sql) do
    Exqlite.Sqlite3.execute(conn, sql)
  end

  defp execute(conn, sql, params) do
    case Sqlite3.prepare(conn, sql) do
      {:ok, stmt} ->
        result =
          with :ok <- Sqlite3.bind(stmt, params),
               :done <- Sqlite3.step(conn, stmt) do
            :ok
          else
            error -> error
          end

        Sqlite3.release(conn, stmt)
        result

      error ->
        error
    end
  end

  defp query(conn, sql, params) do
    case Sqlite3.prepare(conn, sql) do
      {:ok, stmt} ->
        Sqlite3.bind(stmt, params)
        result = Sqlite3.step(conn, stmt)
        Sqlite3.release(conn, stmt)
        result

      _error ->
        nil
    end
  end

  defp create_tables(conn) do
    sql =
      [
        """
        CREATE TABLE IF NOT EXISTS secondary_indexes (
          cf TEXT,
          field TEXT,
          key TEXT,
          value TEXT,
          PRIMARY KEY (cf, field, key)
        )
        """
      ]

    Enum.each(sql, fn s -> execute(conn, s) end)
  end

  defp drop_tables(conn) do
    sql =
      [
        "DROP TABLE IF EXISTS secondary_indexes"
      ]

    Enum.each(sql, fn s -> execute(conn, s) end)
  end

  def backup(conn, filename) do
    case execute(conn, "VACUUM INTO '#{filename}'") do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
