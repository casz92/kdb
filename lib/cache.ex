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
    name = Keyword.get(opts, :name) || make_ref()

    case Kdb.Registry.get_cache(name) do
      nil ->
        t =
          :ets.new(__MODULE__, [
            :set,
            :public,
            read_concurrency: true,
            write_concurrency: true
          ])

        ttl = Keyword.get(opts, :ttl, :undefined)
        cache = %__MODULE__{name: name, ttl: ttl, t: t}
        public = Keyword.get(opts, :public, true)

        if public do
          Kdb.Registry.register(cache)
        end

        cache

      cache ->
        cache
    end
  end

  @spec put(
          cache :: t(),
          bucket :: atom(),
          key :: binary(),
          value :: term(),
          bucket_ttl :: integer() | :infinity
        ) ::
          boolean()
  def put(%__MODULE__{t: t, ttl: ttl}, bucket, key, value, bucket_ttl) do
    :ets.insert(t, {{bucket, key}, value, calc_ttl(ttl, bucket_ttl)})
  end

  @spec update_counter(
          cache :: t(),
          bucket_name :: atom(),
          key :: binary(),
          amount :: integer(),
          default :: term(),
          bucket_ttl :: integer() | :infinity
        ) ::
          integer()
  def update_counter(%__MODULE__{t: t, ttl: ttl}, bucket_name, key, amount, default, bucket_ttl) do
    id = {bucket_name, key}
    :ets.update_counter(t, id, {2, amount}, {id, default, calc_ttl(ttl, bucket_ttl)})
  end

  @spec get(
          cache :: t(),
          bucket_name :: atom(),
          key :: binary(),
          bucket_ttl :: integer() | :infinity
        ) :: term() | nil
  def get(%__MODULE__{t: t, ttl: ttl}, bucket, key, bucket_ttl) do
    id = {bucket, key}

    case :ets.lookup(t, id) do
      [{_key, @key_delete, _timestamp}] ->
        @key_delete

      [{_key, value, _timestamp}] ->
        update_ttl(t, id, calc_ttl(ttl, bucket_ttl))

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

  @spec update(
          cache :: t(),
          bucket :: atom(),
          key :: binary(),
          value :: term(),
          default :: term(),
          bucket_ttl :: integer() | :infinity
        ) ::
          boolean()
  def update(%__MODULE__{t: t, ttl: ttl}, bucket, key, value, default, bucket_ttl) do
    :ets.update_element(
      t,
      {bucket, key},
      {2, value},
      {{bucket, key}, default, calc_ttl(ttl, bucket_ttl)}
    )
  end

  @spec delete(cache :: t(), atom(), binary()) :: true
  def delete(%__MODULE__{t: t}, bucket, id) do
    # :ets.delete(t, {bucket, id})
    :ets.insert(t, {{bucket, id}, @key_delete, 0})
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
  def cleanup(%Kdb.Cache{t: tid}, older_than) do
    n =
      :ets.foldl(
        fn
          {key, @key_delete, _}, acc ->
            :ets.delete(tid, key)
            acc + 1

          {key, _value, readed_at}, acc when is_integer(readed_at) and readed_at < older_than ->
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

  defp calc_ttl(:infinity, _b), do: :infinity
  defp calc_ttl(:undefined, :infinity), do: :infinity
  defp calc_ttl(:undefined, ttl), do: now_add(ttl)
  defp calc_ttl(ttl, _), do: now_add(ttl)

  defp now_add(ttl) do
    :os.system_time(@unit_time) + ttl
  end

  defp update_ttl(_t, _id, ttl) when is_atom(ttl), do: :ok

  defp update_ttl(t, id, ttl) do
    :ets.update_element(t, id, {3, now_add(ttl)})
  end
end
