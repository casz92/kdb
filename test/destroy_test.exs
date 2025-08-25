# mix test test/destroy_test.exs
defmodule DestroyTest do
  use ExUnit.Case, async: false
  doctest Kdb

  import KdbTestUtils

  setup_all do
    open()
  end

  test "destroy", %{kdb: kdb, sup: sup} do
    # destroy database
    assert :ok = Kdb.destroy(kdb)
    assert :ok = Supervisor.stop(sup)
  end
end
