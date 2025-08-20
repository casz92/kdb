defmodule Kdb.Indexer.Stream do
  alias Exqlite.Sqlite3

  def query(conn, sql, params \\ []) do
    Stream.resource(
      fn -> prepare_cursor(conn, sql, params) end,
      fn cursor -> fetch_next(conn, cursor) end,
      fn cursor -> close_cursor(conn, cursor) end
    )
  end

  defp prepare_cursor(conn, sql, params) do
    {:ok, stmt} = Sqlite3.prepare(conn, sql)
    Sqlite3.bind(stmt, params)
    stmt
  end

  defp fetch_next(conn, stmt) do
    case Sqlite3.step(conn, stmt) do
      {:row, row} -> {[row], stmt}
      :done -> {:halt, stmt}
    end
  end

  defp close_cursor(conn, stmt) do
    Sqlite3.release(conn, stmt)
  end
end
