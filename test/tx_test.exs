# mix test test/tx_test.exs
defmodule TxTest do
  use ExUnit.Case, async: false
  import KdbTestUtils

  setup_all do
    open()
  end

  test "transaction", %{kdb: kdb} = params do
    assert :ok ==
             Kdb.transaction(kdb, fn batch ->
               Bucket.put(batch, "jess", 700)
               Bucket.incr(batch, "carlos", 1000)
               Bucket.put(batch, "jim", 950)
               Bucket.put(batch, "caroline", 100)
               Bucket.put(batch, "jony", 500)
             end)

    close(params)
  end
end
