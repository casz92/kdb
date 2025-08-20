defmodule Kdb.Indexer.Stream do
  def query(conn, sql, params \\ []) do
    Stream.resource(
      fn -> prepare_cursor(conn, sql, params) end,
      fn cursor -> fetch_next(conn, cursor) end,
      fn cursor -> close_cursor(conn, cursor) end
    )
  end

  defp prepare_cursor(conn, sql, params) do
    {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, sql)
    Exqlite.Sqlite3.bind(stmt, params)
    stmt
  end

  defp fetch_next(conn, stmt) do
    case Exqlite.Sqlite3.step(conn, stmt) do
      {:row, row} -> {[row], stmt}
      :done -> {:halt, stmt}
    end
  end

  defp close_cursor(conn, stmt) do
    Exqlite.Sqlite3.release(conn, stmt)
  end
end
