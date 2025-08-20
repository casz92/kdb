defmodule Kdb.Behaviour.Batch do
  @callback new :: any()
  @callback save(any(), String.t()) :: :ok | {:error, term()}
  @callback load(String.t(), String.t()) :: any()
  @callback count(any()) :: non_neg_integer()
  @callback commit(any()) :: :ok | {:error, term()}
  @callback release(any()) :: :ok | {:error, term()}
end

defmodule Kdb.Store.Batch do
  import Kdb.Utils
  # @behaviour Kdb.Behaviour.Batch
  defstruct [:store, :batch]

  def new(db) do
    {:ok, batch} = :rocksdb.batch()
    %__MODULE__{batch: batch, store: db}
  end

  def save(%__MODULE__{batch: batch}, filename) do
    binary = :rocksdb.batch_tolist(batch) |> term_to_binary()
    File.write(filename, binary)
  end

  def load(dbfile, filename, buckets) do
    binary = File.read!(filename)
    operations = binary_to_term(binary)

    if byte_size(operations) == 0 do
      {:ok, batch} = :rocksdb.batch()

      {:ok, cfs} =
        :rocksdb.list_column_families(dbfile, [])

      cfs_indexed =
        Enum.map(cfs, fn x ->
          Map.get(buckets, String.Chars.to_string(x) |> String.to_atom())[:handle]
        end)
        |> Enum.with_index(fn element, index -> {index, element} end)
        |> Enum.into(%{})

      Enum.each(operations, fn
        {:put, cf, key, value} ->
          :rocksdb.batch_put(batch, cfs_indexed[cf], key, value)

        {:delete, cf, key} ->
          :rocksdb.batch_delete(batch, cf, key)

        _ ->
          nil
      end)

      batch
    else
      nil
    end
  end

  def count(%__MODULE__{batch: batch}) do
    :rocksdb.batch_count(batch)
  end

  def commit(%__MODULE__{batch: batch, store: db}) do
    if :rocksdb.batch_count(batch) > 0 do
      :rocksdb.write_batch(db, batch, [])
    end

    :ok
  end

  def release(%__MODULE__{batch: batch}) do
    :rocksdb.release_batch(batch)
  end
end
