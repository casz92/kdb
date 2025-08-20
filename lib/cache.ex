defmodule Kdb.Cache do
  defstruct [:name, :ttl, :t]

  @type t :: %__MODULE__{
          name: atom(),
          ttl: integer(),
          t: any()
        }

  @unit_time :millisecond
  @key_delete :delete

  def new(opts) do
    t =
      :ets.new(__MODULE__, [
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])

    ttl = Keyword.get(opts, :ttl, :infinity)
    name = Keyword.get(opts, :name) || make_ref()
    cache = %__MODULE__{name: name, ttl: ttl, t: t}
    public = Keyword.get(opts, :public, true)

    if public do
      Kdb.Registry.register(cache)
    end

    cache
  end

  @spec put(
          cache :: t(),
          bucket :: atom(),
          key :: binary(),
          value :: term()
        ) ::
          boolean()
  def put(%__MODULE__{t: t, ttl: :infinity = ttl}, bucket, key, value) do
    :ets.insert(t, {{bucket, key}, value, ttl})
  end

  def put(%__MODULE__{t: t, ttl: ttl}, bucket, key, value) do
    :ets.insert(t, {{bucket, key}, value, now() + ttl})
  end

  @spec update_counter(
          cache :: t(),
          bucket_name :: atom(),
          key :: binary(),
          amount :: integer(),
          default :: term()
        ) ::
          integer()
  def update_counter(%__MODULE__{t: t, ttl: ttl}, bucket_name, key, amount, default) do
    id = {bucket_name, key}
    :ets.update_counter(t, id, {2, amount}, {id, default, ttl})
  end

  @spec get(cache :: t(), bucket_name :: atom(), key :: binary()) :: term() | nil
  def get(%__MODULE__{t: t}, bucket, key) do
    case :ets.lookup(t, {bucket, key}) do
      [{_key, @key_delete, _timestamp}] ->
        @key_delete

      [{_key, value, _timestamp}] ->
        value

      _ ->
        nil
    end
  end

  def has_key?(%__MODULE__{t: t}, bucket, key) do
    case :ets.lookup(t, {bucket, key}) do
      [{_key, @key_delete, _timestamp}] ->
        false

      [{_key, _value, _timestamp}] ->
        true

      _ ->
        nil
    end
  end

  @spec update(cache :: t(), bucket :: atom(), key :: binary(), value :: term(), default :: term()) ::
          boolean()
  def update(%__MODULE__{t: t, ttl: ttl}, bucket, key, value, default) do
    :ets.update_element(t, {bucket, key}, {2, value}, {{bucket, key}, default, ttl})
  end

  @spec delete(cache :: t(), atom(), binary()) :: true
  def delete(%__MODULE__{t: t}, bucket, id) do
    # :ets.delete(t, {bucket, id})
    :ets.insert(t, {{bucket, id}, @key_delete, 0})
  end

  defp now do
    :os.system_time(@unit_time)
  end

  def cleanup(older_than) do
    tid = :ets.whereis(Kdb.Registry)

    :ets.foldl(
      fn
        {{:batch, _batch_id}, %{cache: cache}}, acc ->
          if cache.ttl != :infinity do
            acc + cleanup(cache, older_than)
          else
            acc
          end

        _, acc ->
          acc
      end,
      0,
      tid
    )
  end

  @spec cleanup(batch :: any(), older_than :: integer()) :: integer()
  def cleanup(%Kdb.Batch{} = batch, older_than) do
    tid = batch.cache.t

    n =
      :ets.foldl(
        fn
          {key, @key_delete, _}, acc ->
            :ets.delete(tid, key)
            acc + 1

          {key, _value, readed_at}, acc when readed_at < older_than ->
            :ets.delete(tid, key)
            acc + 1

          _, acc ->
            acc
        end,
        0,
        tid
      )

    n
  end
end
