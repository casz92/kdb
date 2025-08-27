# mix test test/inspect_test.exs
defmodule InspectTest do
  use ExUnit.Case, async: false
  import KdbTestUtils
  import Kdb.Bucket, only: [put_batch: 2]

  alias Kdb.DefaultBucket

  setup_all do
    open()
  end

  test "inspect", %{kdb: kdb} = params do
    batch = Kdb.Batch.new(name: :inspect, db: kdb)

    myb = Kdb.get_bucket(kdb, :bucket) |> put_batch(batch)
    myb2 = Kdb.get_bucket(kdb, :bucket2) |> put_batch(batch)
    default = Kdb.get_bucket(kdb, :default) |> put_batch(batch)
    bi = Kdb.get_bucket(kdb, :bucket2Index) |> put_batch(batch)

    # inspect items
    myb
    |> Kdb.Bucket.Stream.stream()
    |> Enum.to_list()
    |> IO.inspect(label: "bucket stream")

    myb2
    |> Kdb.Bucket.Stream.stream()
    |> Enum.to_list()
    |> IO.inspect(label: "bucket2 stream")

    default
    |> Enum.to_list()
    |> IO.inspect(label: "default bucket stream")

    DefaultBucket.count_keys(batch) |> IO.inspect(label: "count keys default bucket")
    Bucket2.count_keys(batch, "accounts") |> IO.inspect(label: "count keys accounts")

    default
    |> Kdb.Bucket.Stream.keys(action: :prev, seek: :last)
    |> Enum.to_list()
    |> IO.inspect(label: "reverse keys default bucket")

    bi |> Enum.to_list() |> IO.inspect(label: "bucket2Index list")

    Bucket2.exists?(batch, :name, "John") |> IO.inspect(label: "get_unique name John")

    Bucket2.find(batch, "age", 21, operator: ">=")
    |> Enum.to_list()
    |> IO.inspect(label: "find age 21")

    close(params)
  end
end
