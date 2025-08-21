# mix test test/incr_test.exs
defmodule IncrementTest do
  use ExUnit.Case, async: false

  import KdbTestUtils

  setup_all do
    open()
  end

  test "increment", %{kdb: kdb} = params do
    batch = Kdb.Batch.new(name: :counters, db: kdb)
    c = Bucket.get(batch, "counter") || 0
    Bucket.incr(batch, "counter", 1)
    Bucket.incr(batch, "counter", 2)
    Bucket.incr(batch, "counter", 3)
    assert Bucket.get(batch, "counter") == 6 + c
    assert :ok == Kdb.Batch.commit(batch)

    close(params)
  end
end
