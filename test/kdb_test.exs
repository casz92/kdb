# mix test test/kdb_test.exs
defmodule KdbTest do
  use ExUnit.Case
  doctest Kdb

  defmodule Bucket do
    use Kdb.Bucket, name: :bucket, ttl: true
  end

  defmodule Bucket2 do
    use Kdb.Bucket, name: Bucket2
  end

  test "database and buckets" do
    {:ok, _sup} = Kdb.start_link(name: :dbname, folder: "database", buckets: [Bucket, Bucket2])
    kdb = Kdb.get(:dbname)
    kdb = Kdb.batch(kdb, "first batch")
    myb = Kdb.get_bucket(kdb, :bucket)
    myb2 = Kdb.get_bucket(kdb, Bucket2)
    defult = Kdb.get_bucket(kdb, :default)

    Bucket.put(myb, "mykey", 10)
    DefaultBucket.put(defult, "mykey", 10)
    Bucket.put(myb, "mykey2", "myvalue")
    Bucket.put(myb, "mykey2", :pop)
    Bucket.put(myb2, "mykey2", :pop)
    Bucket.put(myb2, "mymap", %{a: 1, b: 2, c: 3})
    assert true == Bucket.has_key?(myb, "mykey2")
    assert Bucket.incr(myb, "mykey", 7) == 17
    assert Bucket.incr(myb, "mykey", -2) == 15
    Bucket.delete(myb, "mykey2")
    assert false == Bucket.has_key?(myb, "mykey2")
    assert Bucket.get(myb, "mykey") == 15
    assert myb["mykey"] == 15
    assert myb2["mykey"] == nil
    assert :ok == Kdb.commit(kdb, "first batch")

    Kdb.transaction(kdb, fn kdb ->
      myb = Kdb.get_bucket(kdb, :bucket)
      Bucket.put(myb, "jess", 700)
      Bucket.incr(myb, "carlos", 1000)
      Bucket.put(myb, "jim", 950)
      Bucket.put(myb, "caroline", 100)
      Bucket.put(myb, "jony", 500)
    end)

    myb |> Enum.to_list() |> IO.inspect()
    myb2 |> Kdb.Stream.stream() |> Enum.to_list() |> IO.inspect()
    defult |> Enum.to_list() |> IO.inspect()

    assert :ok == Kdb.close(kdb)

    # assert true != Kdb.destroy(kdb)
  end
end
