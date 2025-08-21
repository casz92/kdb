# mix test test/batch_test.exs
defmodule BatchTest do
  use ExUnit.Case, async: false
  # use ExUnit.Case
  doctest Kdb

  import Kdb.Bucket, only: [put_batch: 2]
  import KdbTestUtils

  setup_all do
    open()
  end

  test "batching", %{kdb: kdb} = params do
    batch = Kdb.Batch.new(name: :first, db: kdb)
    myb = Kdb.get_bucket(kdb, :bucket) |> put_batch(batch)

    Bucket.put(batch, "mykey", 10)
    Bucket.put(batch, "mykey2", "myvalue")
    Bucket.put(batch, "mykey2", :pop)
    Bucket.put(batch, "mymap", %{a: 1, b: 2, c: 3})
    Bucket.delete(batch, "mymap")

    DefaultBucket.put_new(batch, "a", %{a: 70, b: 30})
    DefaultBucket.put_new(batch, "b", %{a: 30, b: 70})
    DefaultBucket.put_new(batch, "c", %{a: 55, b: 45})
    DefaultBucket.put_new(batch, "d", %{a: 20, b: 80})

    assert false == Bucket.has_key?(batch, "mymap")
    assert true == Bucket.has_key?(batch, "mykey")
    assert Bucket.get(batch, "mykey") == 10
    assert myb["mykey2"] == :pop

    # List
    batch
    |> Bucket.multi_append("mylist", "item1")
    |> Bucket.multi_append("mylist", "item2")
    |> Bucket.multi_append("mylist", "item3")
    |> Bucket.multi_remove("mylist", ["item2"])

    assert Bucket.includes?(batch, "mylist", "item1") == true
    assert Bucket.includes?(batch, "mylist", "item2") == false
    assert :ok == Kdb.Batch.commit(batch)

    close(params)
  end
end
