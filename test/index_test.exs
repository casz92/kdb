# mix test test/index_test.exs
defmodule IndexTest do
  use ExUnit.Case, async: false

  import KdbTestUtils

  setup_all do
    open()
  end

  test "indexes", %{kdb: kdb} = params do
    batch = Kdb.Batch.new(name: :indexes, db: kdb)
    Bucket2.put(batch, "john_id", %{name: "John", age: 20})
    Bucket2.put(batch, "mike_id", %{name: "Mike", age: 22})
    Bucket2.put_new(batch, "acc_1a5sd4864f5sd1", %{name: "Gary Oldman", age: 47})
    Bucket2.put_new(batch, "acc_1a5sdTRpdsd", %{name: "Robert Takanashi", age: 29})
    Bucket2.put_new(batch, "acc_Poskdnmkeer45", %{name: "Beth Green", age: 28})
    Bucket2.delete(batch, "james_idx")
    Bucket2.put(batch, "james_id", %{name: "James", age: 21})
    assert false == Bucket2.put(batch, "james_idx", %{name: "James", age: 27})
    assert :ok == Kdb.Batch.commit(batch)

    close(params)
  end
end
