defmodule Kdb.Registry do
  @table __MODULE__
  # @bucket :bucket

  def child_spec(_) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :init, []},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

  def init do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
    end

    :ignore
  end

  # {bdname, kdb}
  # {{bdname, bucket_name}, bucket}
  # {{bdname, :global, :batch}, value}
  # {{bdname, bucket_name, :batch}, value}
  def register(name, %Kdb{} = kdb) do
    :ets.insert(@table, {name, kdb})

    for bucket <- kdb.buckets do
      :ets.insert(@table, {{name, bucket.name}, bucket})
    end

    :ok
  end

  def register(name, value) do
    :ets.insert(@table, {name, value})

    :ok
  end

  def update(name, kdb) do
    :ets.update_element(@table, name, {2, kdb})
  end

  def unregister({:batch, _} = key) do
    :ets.delete(@table, key)
  end

  def unregister(name) do
    :ets.delete(@table, name)

    :ets.foldl(
      fn
        {^name, _}, acc ->
          :ets.delete(@table, name)
          acc

        {^name, _, _}, acc ->
          :ets.delete(@table, name)
          acc

        _, acc ->
          acc
      end,
      :ok,
      @table
    )
  end

  def lookup(name) do
    case :ets.lookup(@table, name) do
      [{^name, kdb}] -> {:ok, kdb}
      [] -> :error
    end
  end

  def lookup(name, bucket_name) do
    case :ets.lookup(@table, {name, bucket_name}) do
      [{^name, ^bucket_name, bucket}] -> {:ok, bucket}
      [] -> :error
    end
  end
end
