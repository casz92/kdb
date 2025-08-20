# mix test test/restore_test.exs
defmodule RestoreTest do
  use ExUnit.Case, async: false

  test "restore" do
    if File.exists?("backup.zip") do
      assert :ok = Kdb.restore("backup.zip", "database")
    else
      IO.inspect("No backup file found to restore", label: "Restore Status")
    end
  end
end
