defmodule Kdb.Registry do
  @table __MODULE__
  @key :kdb

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
    if :ets.info(@table) == :undefined do
      :ets.new(@table, [
        :set,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])
    end

    :ignore
  end

  # {:db, name}
  # {:bucket, dbname, bucket_name}
  # {:batch, name}
  # {:cache, name}
  def register(%Kdb{name: name} = kdb) do
    # :ets.insert(@table, {name, kdb})
    :persistent_term.put({@key, name}, kdb)

    # Enum.each(buckets, fn bucket ->
    #   :ets.insert(@table, {{:db, name}, bucket})
    # end)
  end

  def register(%Kdb.Bucket{name: name, dbname: dbname} = bucket) do
    kdb = get_db(dbname)

    :persistent_term.put({@key, dbname}, %{
      kdb
      | buckets: Map.put(kdb.buckets, name, bucket)
    })
  end

  def register(%Kdb.Batch{} = batch) do
    :ets.insert(@table, {{:batch, batch.name}, batch})
  end

  def register(%Kdb.Cache{} = cache) do
    :ets.insert(@table, {{:cache, cache.name}, cache})
  end

  def unregister(%Kdb{name: name}) do
    :persistent_term.erase({@key, name})
  end

  def unregister(%Kdb.Bucket{name: name, dbname: dbname}) do
    kdb = get_db(dbname)
    :persistent_term.put({@key, dbname}, %{kdb | buckets: Map.delete(kdb.buckets, name)})
  end

  def unregister(%Kdb.Batch{} = batch) do
    :ets.delete(@table, {:batch, batch.name})
  end

  def unregister(%Kdb.Cache{} = cache) do
    :ets.delete(@table, {:cache, cache.name})
  end

  def unregister(name) do
    :ets.delete(@table, name)
  end

  def get_db(name) do
    :persistent_term.get({@key, name}, nil)
  end

  def get_bucket(dbname, bucket_name) do
    case :persistent_term.get({@key, dbname}, nil) do
      nil -> nil
      %Kdb{buckets: buckets} -> Map.get(buckets, bucket_name)
    end
  end

  def get_batch(name) do
    case :ets.lookup(@table, {:batch, name}) do
      [{_id, object}] -> object
      [] -> nil
    end
  end

  def get_cache(name) do
    case :ets.lookup(@table, {:cache, name}) do
      [{_id, object}] -> object
      [] -> nil
    end
  end
end
