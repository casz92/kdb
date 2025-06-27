defmodule Kdb.Bucket do
  defstruct [:db, :name, :dbname, :handle, :t, :batch, :module]

  @type t :: %__MODULE__{
          db: reference(),
          name: atom(),
          dbname: atom(),
          handle: reference() | nil,
          t: :ets.tid() | nil,
          batch: reference() | nil,
          module: module()
        }

  defmacro __using__(opts) do
    bucket = Keyword.get(opts, :name) || raise ArgumentError, "missing :name option"
    cache = Keyword.get(opts, :cache, Kdb.Cache)
    ttl = Keyword.get(opts, :ttl, true)
    decoder = Keyword.get(opts, :decoder, &Kdb.binary_to_term/1)
    encoder = Keyword.get(opts, :encoder, &Kdb.term_to_binary/1)

    quote bind_quoted: [
            bucket: bucket,
            cache: cache,
            ttl: ttl,
            decoder: decoder,
            encoder: encoder
          ] do
      @bucket bucket
      @cache cache
      @ttl ttl
      @decoder decoder
      @encoder encoder

      def new(dbname, db, handle) do
        t =
          :ets.new(__MODULE__, [
            :set,
            :public,
            read_concurrency: true,
            write_concurrency: true
          ])

        %Kdb.Bucket{
          dbname: dbname,
          db: db,
          name: @bucket,
          handle: handle,
          t: t,
          module: __MODULE__
        }
      end

      def name, do: @bucket
      def ttl, do: @ttl
      def decoder(x), do: @decoder.(x)
      def encoder(x), do: @encoder.(x)

      def batch(%Kdb.Bucket{dbname: dbname} = bucket, name) do
        batch = Kdb.batch(name)
        %{bucket | batch: batch}
      end

      def put(bucket, key, value) do
        :ets.insert(bucket.t, {key, value})
        :rocksdb.batch_put(bucket.batch, bucket.handle, key, @encoder.(value))
        @ttl and @cache.put(@bucket, key)
      end

      defp put_in_memory(bucket, key, value) do
        :ets.insert(bucket.t, {key, value})
        @ttl and @cache.put(@bucket, key)
      end

      def get(bucket, key) do
        case :ets.lookup(bucket.t, key) do
          [{^key, value}] ->
            value

          [] ->
            get_from_disk(bucket, key)
        end
      end

      def has_key?(bucket, key) do
        case :ets.member(bucket.t, key) do
          true ->
            true

          false ->
            case get_from_disk(bucket, key) do
              nil -> false
              _value -> true
            end
        end
      end

      def get_from_disk(bucket, key) do
        case :rocksdb.get(bucket.db, bucket.handle, key, []) do
          {:ok, value} ->
            result = @decoder.(value)
            put_in_memory(bucket, key, result)
            result

          :not_found ->
            nil
        end
      end

      def incr(bucket, key, amount) when is_integer(amount) do
        result = get(bucket, key) || 0
        result = :ets.update_counter(bucket.t, key, {2, amount}, {key, result})
        :rocksdb.batch_put(bucket.batch, bucket.handle, key, @encoder.(result))
        result
      end

      def delete(bucket, key) do
        :ets.delete(bucket.t, key)
        :rocksdb.batch_delete(bucket.batch, bucket.handle, key)
        @ttl and @cache.delete(@bucket, key)
      end
    end
  end

  defimpl Enumerable do
    def reduce(bucket, acc, fun) do
      # ← puedes parametrizar el nombre de tabla
      stream = Kdb.Stream.stream(bucket, decoder: &bucket.module.decoder/1)
      Enumerable.reduce(stream, acc, fun)
    end

    def count(bucket) do
      # No se puede contar sin recorrer todo
      {:error, bucket.module}
    end

    def member?(bucket, _element) do
      # No implementado de forma eficiente
      {:error, bucket.module}
    end

    def slice(bucket) do
      {:error, bucket.module}
    end
  end

  def fetch(bucket = %Kdb.Bucket{module: module}, key) do
    case :ets.lookup(bucket.t, key) do
      [{^key, value}] ->
        {:ok, value}

      [] ->
        case module.get_from_disk(bucket, key) do
          nil -> :error
          value -> {:ok, value}
        end
    end
  end

  def get_and_update(bucket = %Kdb.Bucket{module: module}, key, fun) do
    case module.get(bucket, key) do
      nil ->
        {:ok, nil, nil}

      value ->
        result = fun.(value)
        module.put(bucket, key, result)
        {:ok, value, result}
    end
  end

  defimpl Inspect do
    def inspect(bucket, _opts) do
      "#Kdb.Bucket<name: #{bucket.name}>"
    end
  end
end

defmodule Kdb.Stream do
  def stream(%Kdb.Bucket{db: db, handle: handle, module: module}, opts \\ []) do
    # <<>> or <<0>> or :last
    initial_seek = Keyword.get(opts, :seek, <<>>)
    # :next or :prev
    direction = Keyword.get(opts, :direction, :next)
    decoder_fun = Keyword.get(opts, :decoder, &module.decoder/1)

    Stream.resource(
      # Start: open iterator and seek
      fn ->
        {:ok, iter} = :rocksdb.iterator(db, handle, [])

        state =
          case :rocksdb.iterator_move(iter, initial_seek) do
            {:ok, key, value} -> {:ok, iter, key, value}
            _ -> {:done, iter}
          end

        state
      end,

      # Next: return {k, v} and move iterator
      fn
        {:done, iter} ->
          {:halt, iter}

        {:ok, iter, key, value} ->
          item = {key, decoder_fun.(value)}

          next =
            case :rocksdb.iterator_move(iter, direction) do
              {:ok, next_key, next_val} -> {:ok, iter, next_key, next_val}
              _ -> {:done, iter}
            end

          {[item], next}
      end,

      # After: close iterator
      fn iter ->
        :ok = :rocksdb.iterator_close(iter)
      end
    )
  end
end

defmodule DefaultBucket do
  use Kdb.Bucket, name: :default, ttl: true
end
