# mix test test/ttl_test.exs
defmodule TtlTest do
  use ExUnit.Case, async: false
  doctest Kdb

  import KdbTestUtils

  setup_all do
    open()
  end

  test "ttl", %{kdb: kdb} = params do
    batch = Kdb.Batch.new(name: :first, db: kdb, cache: [name: :temp])

    batch
    |> Bucket.multi_put("a1", "item1")
    |> Bucket.multi_put("a2", "item2")
    |> Bucket.multi_put("a3", 59856)
    |> Bucket.multi_append("a4", ["item2", 157, :pop])

    batch
    |> Bucket2.multi_put("acc_psE8Fl92TMn", %{name: "Stephanie Johnson", age: 27})
    |> Bucket2.multi_put("acc_F84Eplf4Rt", %{name: "Michelle Beckman", age: 27})

    t = batch.cache.t
    lista = :ets.tab2list(t)
    IO.inspect(lista, label: "Records")

    IO.puts("Wait 2s")
    :timer.sleep(2000)
    Kdb.Scheduler.cleanup(nil)

    listb = :ets.tab2list(t)
    IO.inspect(listb, label: "Records after clean")
    assert length(listb) == 5

    close(params)
  end
end
