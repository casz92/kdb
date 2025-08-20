# mix test test/kdb_test.exs
defmodule KdbTest do
  use ExUnit.Case
  # use ExUnit.Case
  doctest Kdb

  defmodule Bucket do
    use Kdb.Bucket, name: :bucket, ttl: 1_000
  end

  defmodule Bucket2Index do
    use Kdb.Bucket, name: :bucket2index, ttl: 60_000
  end

  defmodule Bucket2 do
    use Kdb.Bucket,
      name: Bucket2,
      unique: [{:name, Bucket2Index}],
      secondary: [:age],
      ttl: 1_000
  end

  # if File.exists?("database") do
  #   :rocksdb.list_column_families(~c"database", []) |> IO.inspect(label: "list_column_families")
  # end

  test "database and buckets" do
    opts = [name: :dbname, folder: "database", buckets: [Bucket, Bucket2Index, Bucket2]]
    {:ok, sup} = Kdb.start_link(opts)
    kdb = Kdb.get(:dbname)
    batch = Kdb.Batch.new(name: :first, db: kdb)

    Bucket.put(batch, "mykey", 10)
    DefaultBucket.put(batch, "mykey", 10)
    Bucket2.put(batch, "john_id", %{name: "John", age: 20})
    Bucket2.put(batch, "mike_id", %{name: "Mike", age: 22})
    Bucket2.delete(batch, "james_idx")
    Bucket2.put(batch, "james_id", %{name: "James", age: 21})
    assert false == Bucket2.put(batch, "james_idx", %{name: "James", age: 27})
    Bucket.put(batch, "mykey2", "myvalue")
    Bucket.put(batch, "mykey2", :pop)
    Bucket.put(batch, "mykey2", :pop)
    Bucket.put(batch, "mymap", %{a: 1, b: 2, c: 3})
    assert Bucket.incr(batch, "mykey", 7) == 17
    assert Bucket.incr(batch, "mykey", -2) == 15
    Bucket.delete(batch, "mykey2")
    assert false == Bucket.has_key?(batch, "mykey2")
    assert Bucket.get(batch, "mykey") == 15

    myb = Kdb.get_bucket(kdb, :bucket)
    myb2 = Kdb.get_bucket(kdb, :bucket2)
    defult = Kdb.get_bucket(kdb, :default)
    bi = Kdb.get_bucket(kdb, :bucket2index)
    assert true == is_struct(myb)

    # transaction
    assert :ok ==
             Kdb.transaction(kdb, fn batch ->
               Bucket.put(batch, "jess", 700)
               Bucket.incr(batch, "carlos", 1000)
               Bucket.put(batch, "jim", 950)
               Bucket.put(batch, "caroline", 100)
               Bucket.put(batch, "jony", 500)
             end)

    myb = %{myb | batch: batch}
    assert myb["mykey"] == 15

    # List
    Bucket.get(batch, "mylist")
    # |> IO.inspect(label: "mylist after append and remove")
    batch
    |> Bucket.multi_append("mylist", "item1")
    |> Bucket.multi_append("mylist", "item2")
    |> Bucket.multi_append("mylist", "item3")
    |> Bucket.multi_remove("mylist", ["item2"])

    assert Bucket.includes?(batch, "mylist", "item1") == true
    assert Bucket.includes?(batch, "mylist", "item2") == false
    assert :ok == Kdb.Batch.commit(batch)

    # inspect items
    myb |> Kdb.Bucket.Stream.stream() |> Enum.to_list() |> IO.inspect(label: "myb stream")
    myb2 |> Kdb.Bucket.Stream.stream() |> Enum.to_list() |> IO.inspect(label: "myb2 stream")
    defult |> Enum.to_list() |> IO.inspect(label: "default bucket stream")

    Bucket2.exists?(batch, :name, "John") |> IO.inspect(label: "get_unique name John")

    Bucket2.find(batch, "age", 21, operator: ">=")
    |> Enum.to_list()
    |> IO.inspect(label: "find age 21")

    bi |> Enum.to_list() |> IO.inspect(label: "bucket2index")

    Kdb.backup(kdb, "backup")
    assert :ok == Kdb.close(kdb)
    assert :ok == Supervisor.stop(sup)
    assert :ok == Kdb.destroy(kdb)
  end

  # test "ttl" do
  #   {:ok, sup} = Kdb.start_link(name: :dbname, folder: "database", buckets: [Bucket, Bucket2])
  #   kdb = Kdb.get(:dbname)
  #   kdb = Kdb.batch(kdb, :ttl)
  #   myb = Kdb.get_bucket(kdb, :bucket)
  #   Bucket.put(myb, "mykey", 10)
  #   assert Bucket.get(myb, "mykey") == 10
  #   Process.sleep(3000)
  #   assert :ets.member(myb.t, "mykey") == true
  #   Kdb.Scheduler.cleanup(nil)
  #   assert :ets.member(myb.t, "mykey") == false
  #   Kdb.release_batch(:ttl)
  #   assert :ok == Kdb.close(kdb)
  #   assert :ok == Supervisor.stop(sup)
  # end
end
