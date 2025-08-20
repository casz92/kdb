defmodule Kdb.Indexer.Batch do
  # @behaviour Kdb.Behaviour.Batch

  defstruct [:conn, :t]

  def new(opts) do
    tid = :ets.new(:batch, [:ordered_set, :public, write_concurrency: true])

    %__MODULE__{
      conn: Keyword.get(opts, :conn) || raise(ArgumentError, "`conn` is required"),
      t: tid
    }
  end

  def add(batch, operation, args) do
    id = :erlang.unique_integer([:monotonic])
    :ets.insert(batch.t, {id, operation, args})
  end

  def count(batch) do
    :ets.info(batch.t, :size)
  end

  def commit(batch) do
    operations = :ets.tab2list(batch.t)
    :ets.delete(batch.t)
    conn = batch.conn

    # operations |> IO.inspect(label: "operations")
    # Kdb.Indexer.begin(conn) |> IO.inspect(label: "Indexer begin")

    Enum.each(operations, fn {_id, ope, args} ->
      apply(Kdb.Indexer, ope, [conn | args])
      # |> IO.inspect(label: "Indexer operation #{ope} #{inspect(args)}")
    end)

    # Kdb.Indexer.commit(conn) |> IO.inspect(label: "Indexer commit")
  end

  def discard(batch) do
    :ets.delete_all_objects(batch.t)
  end

  def release(batch) do
    :ets.delete(batch.t)
  end
end
