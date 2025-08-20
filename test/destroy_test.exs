# mix test test/destroy_test.exs
defmodule DestroyTest do
  use ExUnit.Case, async: false
  # use ExUnit.Case
  doctest Kdb

  import KdbTestUtils

  setup_all do
    open()
  end

  @backup_file "backup.zip"
  test "destroy", %{kdb: kdb, sup: sup} do
    # destroy backup file also
    File.exists?(@backup_file) and File.rm(@backup_file)
    # destroy database
    assert :ok = Kdb.destroy(kdb)
    assert :ok = Supervisor.stop(sup)
  end
end
