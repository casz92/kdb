# mix test test/clean_test.exs
defmodule CleanTest do
  use ExUnit.Case, async: false
  doctest Kdb

  @backup_file "backup.zip"
  @database "database"
  test "clean" do
    # clean all test data
    File.exists?(@backup_file) and File.rm(@backup_file)
    File.exists?(@database) and File.rm_rf(@database)
  end
end
