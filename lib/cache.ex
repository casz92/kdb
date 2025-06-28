defmodule Kdb.Cache do
  @table_name __MODULE__
  @unit_time :millisecond

  def child_spec(_) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :init, []},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

  @doc """
  Inicia el módulo de hits creando la tabla ETS.
  """
  def init() do
    # Crear tabla ETS con nombre del módulo, pública y con concurrencia de lectura
    if :ets.whereis(@table_name) == :undefined do
      :ets.new(@table_name, [
        :set,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])
    end

    :ignore
  end

  @spec put(database :: atom(), bucket_name :: atom(), key :: binary(), ttl :: integer()) ::
          boolean()
  def put(dbname, bucket, id, ttl) do
    timestamp = now() + ttl
    :ets.insert(@table_name, {{id, bucket, dbname}, timestamp})
  end

  @spec delete(atom(), atom(), binary()) :: true
  def delete(dbname, bucket, id) do
    :ets.delete(@table_name, {id, bucket, dbname})
  end

  defp now do
    :os.system_time(@unit_time)
  end

  @spec cleanup(older_than :: integer()) :: integer()
  def cleanup(older_than) do
    n =
      :ets.foldl(
        fn
          {key = {id, bucket_name, dbname}, readed_at}, acc when readed_at < older_than ->
            kdb = Kdb.get(dbname)
            tid = Map.get(kdb.buckets, bucket_name).t
            :ets.delete(tid, id)
            :ets.delete(@table_name, key)
            acc + 1

          _, acc ->
            acc
        end,
        0,
        @table_name
      )

    n
  end

  @callback init() :: :ignore
  @callback put(database :: atom(), bucket :: atom(), id :: binary(), ttl :: integer()) ::
              boolean()
  @callback delete(database :: atom(), bucket :: atom(), id :: binary()) :: any()
  @callback cleanup(older_than :: integer()) :: integer()
end
