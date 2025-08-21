defmodule Kdb.Bucket do
  defstruct [:name, :dbname, :handle, :module, :batch, :ttl]

  @type t :: %__MODULE__{
          name: atom(),
          dbname: atom(),
          handle: reference() | nil,
          module: module(),
          batch: Kdb.Batch.t() | nil,
          ttl: integer()
        }

  defmacro __using__(opts) do
    bucket = Keyword.get(opts, :name) || raise ArgumentError, "missing :name option"
    cache = Keyword.get(opts, :cache, Kdb.Cache)
    ttl = Keyword.get(opts, :ttl, 300_000)
    unique = Keyword.get(opts, :unique, [])
    secondary = Keyword.get(opts, :secondary, [])
    decoder = Keyword.get(opts, :decoder, &Kdb.Utils.binary_to_term/1)
    encoder = Keyword.get(opts, :encoder, &Kdb.Utils.term_to_binary/1)
    stats = Keyword.get(opts, :stats, Kdb.Stats)

    quote bind_quoted: [
            bucket: bucket,
            cache: cache,
            ttl: ttl,
            unique: unique,
            secondary: secondary,
            decoder: decoder,
            encoder: encoder,
            stats: stats
          ] do
      @bucket bucket |> Kdb.Utils.to_bucket_name()
      @cache cache
      @ttl ttl
      @decoder decoder
      @encoder encoder
      @cacheable is_integer(ttl)
      @unique unique
      @secondary secondary
      @has_unique length(unique) > 0
      @has_secondary length(secondary) > 0
      @has_index @has_unique or @has_secondary
      @unique_map Enum.into(@unique, %{}, & &1)
      @stats stats
      @has_stats stats != false
      @info_keys "#{@bucket}:keys"

      @compile {:inline, transform: 5, put_new: 3, get: 3, incr: 4, encoder: 1, decoder: 1}

      alias Kdb.Batch
      alias Kdb.Cache
      alias Kdb.Indexer

      def new(opts) do
        dbname = Keyword.fetch!(opts, :dbname)
        handle = Keyword.fetch!(opts, :handle)
        batch = Keyword.get(opts, :batch)

        %Kdb.Bucket{
          dbname: dbname,
          name: @bucket,
          handle: handle,
          module: __MODULE__,
          batch: batch,
          ttl: @ttl
        }
      end

      def name, do: @bucket
      def ttl, do: @ttl
      def indexes, do: (@unique |> Enum.map(fn {f, _mod} -> f end)) ++ @secondary
      def decoder(x), do: @decoder.(x)
      def encoder(x), do: @encoder.(x)

      def count_keys(batch) do
        @stats.get(batch, @info_keys, 0)
      end

      if @has_index do
        def put(
              batch = %Kdb.Batch{
                db:
                  db = %Kdb{
                    buckets: %{@bucket => %Kdb.Bucket{name: bucket_name, handle: handle}}
                  },
                cache: cache,
                store: store,
                indexer: indexer
              },
              key,
              value
            ) do
          if put_unique(batch, key, value) do
            :rocksdb.batch_put(store.batch, handle, key, @encoder.(value))
            @has_secondary and put_secondary(indexer, key, value)
            @cache.put(cache, bucket_name, key, value)
          else
            false
          end
        end

        if @has_unique do
          defp put_unique(batch, key, value) do
            results =
              Enum.map(@unique, fn {field, module} ->
                val = Map.get(value, field)

                result = module.get(batch, val)
                {result, module, val}
              end)

            if Enum.any?(results, fn {x, _mod, _v} -> x != nil end) do
              false
            else
              Enum.each(results, fn {_r, module, val} ->
                module.put(batch, val, key)
              end)

              true
            end
          end
        else
          def put_unique(_, _, _), do: true
        end

        defp put_secondary(indexer, key, value) do
          Enum.each(@secondary, fn field ->
            value = Map.get(value, field)
            # IO.inspect(value, label: "Secondary Index Value for #{field}")

            value != nil and
              Indexer.Batch.add(indexer, :create_index, [@bucket, field, key, value])
          end)
        end
      else
        def put(
              %Kdb.Batch{
                db:
                  db = %Kdb{
                    buckets: %{@bucket => %Kdb.Bucket{name: bucket_name, handle: handle}}
                  },
                cache: cache,
                store: store,
                indexer: indexer
              },
              key,
              value
            ) do
          :rocksdb.batch_put(store.batch, handle, key, @encoder.(value))
          @cache.put(cache, bucket_name, key, value)
        end
      end

      def put_new(batch, key, value) do
        case get(batch, key) do
          nil ->
            put(batch, key, value) and @stats.incr(batch, @info_keys, 1)

          _value ->
            false
        end
      end

      def get(
            %Kdb.Batch{
              db: %Kdb{
                buckets: %{@bucket => %Kdb.Bucket{name: bucket_name, handle: handle}}
              },
              cache: cache
            } = batch,
            key,
            default \\ nil
          ) do
        case @cache.get(cache, bucket_name, key) do
          :delete ->
            default

          nil ->
            get_from_disk(batch, handle, key, default)

          value ->
            value
        end
      end

      def has_key?(
            %Kdb.Batch{
              db: %Kdb{
                buckets: %{@bucket => %Kdb.Bucket{name: bucket_name, handle: handle} = bucket}
              },
              cache: cache
            } = batch,
            key
          ) do
        case @cache.has_key?(cache, bucket_name, key) do
          nil ->
            case get_from_disk(batch, handle, key) do
              nil -> false
              _value -> true
            end

          result ->
            result
        end
      end

      defp get_from_disk(
             %Kdb.Batch{
               db: db,
               cache: cache
             },
             handle,
             key,
             default \\ nil
           ) do
        case :rocksdb.get(db.store, handle, key, []) do
          {:ok, value} ->
            result = @decoder.(value)
            # put in memory cache
            @cache.put(cache, @bucket, key, result)
            result

          :not_found ->
            default
        end
      end

      def incr(
            batch = %Kdb.Batch{
              db: %Kdb{
                buckets: %{@bucket => %Kdb.Bucket{handle: handle}}
              },
              cache: cache,
              store: store
            },
            key,
            amount,
            initial \\ 0
          )
          when is_integer(amount) do
        raw_batch = store.batch
        old_result = get(batch, key, initial)

        # if initial == old_result do
        #   :rocksdb.batch_put(raw_batch, handle, key, @encoder.(initial))
        # end

        # :rocksdb.batch_merge(raw_batch, handle, key, @encoder.({:int_add, amount}))
        result = @cache.update_counter(cache, @bucket, key, amount, old_result)
        :rocksdb.batch_put(raw_batch, handle, key, @encoder.(result))

        result
      end

      def append(batch, key, new_item) do
        transform(
          batch,
          key,
          new_item,
          # :list_append,
          [],
          fn old_list, new_item ->
            old_list ++ [new_item]
          end
        )
      end

      def remove(batch, key, items) when is_list(items) do
        transform(
          batch,
          key,
          items,
          # :list_substract,
          [],
          fn old_list, items ->
            old_list -- items
          end
        )
      end

      def includes?(batch, key, item) do
        case get(batch, key) do
          nil -> false
          list when is_list(list) -> item in list
          _ -> false
        end
      end

      defp transform(
             batch = %Kdb.Batch{
               db: %Kdb{
                 buckets: %{@bucket => %Kdb.Bucket{handle: handle}}
               },
               cache: cache,
               store: %{batch: raw_batch}
             },
             key,
             new_item,
             #  operation,
             initial,
             fun
           ) do
        old_result = get(batch, key) || initial

        # if initial == old_result do
        #   :rocksdb.batch_put(raw_batch, handle, key, @encoder.(initial))
        # end

        # :rocksdb.batch_merge(raw_batch, handle, key, @encoder.({operation, new_item}))

        new_value = fun.(old_result, new_item)
        :rocksdb.batch_put(raw_batch, handle, key, @encoder.(new_value))
        @cache.update(cache, @bucket, key, new_value, new_value)
      end

      def delete(
            batch = %Kdb.Batch{
              db: %Kdb{
                buckets: %{@bucket => %Kdb.Bucket{handle: handle}}
              },
              indexer: indexer,
              cache: cache,
              store: store
            },
            key
          ) do
        :rocksdb.batch_delete(store.batch, handle, key)
        @has_unique and delete_unique(batch, key)
        @has_secondary and delete_secondary(indexer, key)
        @cache.delete(cache, @bucket, key)
        @has_stats and @stats.incr(batch, @info_keys, -1)
      end

      defp delete_unique(batch, key) do
        Enum.each(@unique, fn {field, module} ->
          case get(batch, key) do
            nil ->
              nil

            map ->
              val = Map.get(map, field)
              module.delete(batch, val)
          end
        end)
      end

      defp delete_secondary(indexer = %Indexer.Batch{t: t}, key) do
        Indexer.Batch.add(indexer, :delete_index, [@bucket, key])
      end

      if @has_index do
        def get_unique(batch, field, val) do
          case @unique_map[field] do
            nil ->
              nil

            module ->
              module.get(batch, val)
          end
        end

        def exists?(batch, field, val) do
          case @unique_map[field] do
            nil ->
              false

            module ->
              module.has_key?(batch, val)
          end
        end

        def find(%Kdb.Batch{indexer: %{conn: conn}} = batch, attr, text, opts \\ []) do
          Kdb.Indexer.find(conn, @bucket, attr, text, opts)
          |> Stream.map(fn [result] ->
            get(batch, result)
          end)
        end
      end

      def drop(%Kdb.Bucket{dbname: dbname, handle: handle}) do
        db = Kdb.get(dbname)
        :rocksdb.drop_column_family(db, handle)
      end

      ## Multi API
      def multi_put(batch, key, value) do
        put(batch, key, value)
        batch
      end

      def multi_delete(batch, key) do
        delete(batch, key)
        batch
      end

      def multi_append(batch, key, new_item) do
        append(batch, key, new_item)
        batch
      end

      def multi_remove(batch, key, items) do
        remove(batch, key, items)
        batch
      end
    end
  end

  @default_batch :default
  def fetch(%Kdb.Bucket{dbname: dbname, module: module, batch: batch}, key) do
    batch = batch || Kdb.Batch.new(name: @default_batch, db: Kdb.get(dbname))

    case module.get(batch, key) do
      nil -> {:error, :not_found}
      value -> {:ok, value}
    end
  end

  def get_and_update(
        %Kdb.Bucket{dbname: dbname, module: module, batch: batch},
        key,
        fun
      ) do
    batch = batch || Kdb.Batch.new(name: @default_batch, db: Kdb.get(dbname))

    case module.get(batch, key) do
      nil ->
        {:ok, nil, nil}

      value ->
        result = fun.(value)
        module.put(batch, key, result)
        {:ok, value, result}
    end
  end

  def put_batch(bucket, batch) when is_atom(batch) or is_reference(batch) do
    %{bucket | batch: Kdb.Batch.new(name: batch, db: Kdb.get(bucket.dbname))}
  end

  def put_batch(bucket, batch) do
    %{bucket | batch: batch}
  end

  @doc """
  Creates a new bucket module with the given name and options.
  The module will use `Kdb.Bucket` and the options provided.
  """
  def make_bucket_module(mod_name, opts) when is_atom(mod_name) and is_list(opts) do
    quoted =
      quote do
        use Kdb.Bucket, unquote_splicing(opts)
      end

    Module.create(mod_name, quoted, Macro.Env.location(__ENV__))
  end
end

defimpl Inspect, for: Kdb.Bucket do
  def inspect(bucket, _opts) do
    "#Kdb.Bucket<name: #{bucket.name}>"
  end
end

defimpl Enumerable, for: Kdb.Bucket do
  def reduce(bucket, acc, fun) do
    # ← puedes parametrizar el nombre de tabla
    stream = Kdb.Bucket.Stream.stream(bucket)
    Enumerable.reduce(stream, acc, fun)
  end

  def count(bucket) do
    # No implemented yet
    {:error, bucket.module}
  end

  def member?(bucket, _element) do
    # No implemented yet
    {:error, bucket.module}
  end

  def slice(bucket) do
    {:error, bucket.module}
  end
end

defmodule DefaultBucket do
  use Kdb.Bucket, name: :default
end

defmodule Kdb.Stats do
  use Kdb.Bucket, name: :stats
end
