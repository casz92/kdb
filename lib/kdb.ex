defmodule Kdb do
  @type tname :: atom() | binary()

  @type t :: %__MODULE__{
          db: reference() | nil,
          name: atom(),
          folder: charlist(),
          batch: reference() | nil,
          buckets: %{tname() => Kdb.Bucket.t()},
          global: Kdb.Bucket.t()
        }

  defstruct [:db, :name, :folder, :batch, :buckets, :global]
  @key :kdb
  # @stat_count "$count"
  @default_cfs ~c"default"

  @open_options [
    create_if_missing: true,
    merge_operator: :erlang_merge_operator
  ]

  @compile {:inline,
   [
     # has_key?: 3,
     # get: 3,
     # fetch: 3,
     # put: 4,
     # incr: 4,
     # delete: 3,
     # total: 2,
     binary_to_term: 1,
     term_to_binary: 1
   ]}

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  def start_link(opts) do
    Kdb.Supervisor.start_link(opts)
  end

  @doc """
  Open a new database.

  ## Options
    * `:folder` - The folder to store the database.
    * `:buckets` - A list of buckets to create.
  """
  @spec new(dbname :: atom(), otps :: keyword()) :: t()
  def new(dbname, opts) do
    folder = Keyword.get(opts, :folder) || raise(ArgumentError, "`folder` is required")
    modules = Keyword.get(opts, :buckets, [])
    folder = to_charlist(folder)

    # open database and load/create column families
    {db, cfs, default_cf} =
      if File.exists?(folder) do
        # check column families
        {:ok, column_families} = :rocksdb.list_column_families(folder, [])

        column_families_mod = Enum.map(modules, &to_charlist(&1.name()))

        columns_to_create =
          column_families_mod -- column_families

        column_families_to_load = column_families_mod -- columns_to_create

        cfs_opts = [
          {@default_cfs, []} | Enum.map(column_families_to_load, &{&1, []})
        ]

        # open database with column families
        {:ok, db, [default_cf | cfs]} =
          :rocksdb.open(folder, @open_options, cfs_opts)

        # create column families
        cfsh =
          for name <- columns_to_create do
            {:ok, handle} = :rocksdb.create_column_family(db, name, [])
            handle
          end

        {db, cfs ++ cfsh, default_cf}
      else
        try do
          {:ok, db, [default_cf | _cfs]} =
            :rocksdb.open(folder, @open_options, [{@default_cfs, []}])

          cfs =
            Enum.map(modules, fn mod ->
              name = mod.name() |> to_charlist()
              {:ok, handle} = :rocksdb.create_column_family(db, name, [])
              handle
            end)

          {db, cfs, default_cf}
        rescue
          e ->
            File.rm_rf!(folder)
            reraise e, __STACKTRACE__
        end
      end

    # load default bucket
    default_bucket = DefaultBucket.new(dbname, db, default_cf)

    # load buckets
    buckets =
      Enum.zip(modules, cfs)
      |> Enum.map(fn {mod, handle} ->
        bucket = mod.new(dbname, db, handle)
        {mod.name(), bucket}
      end)
      |> Map.new()
      |> Map.put(DefaultBucket.name(), default_bucket)

    kdb = %__MODULE__{db: db, name: dbname, folder: folder, buckets: buckets}
    :persistent_term.put({@key, dbname}, kdb)
    kdb
  end

  def get(name) do
    :persistent_term.get({@key, name})
  end

  def get_bucket(%Kdb{buckets: buckets}, name) do
    Map.get(buckets, name)
  end

  def lookup_bucket(dbname, name) do
    Kdb.Registry.lookup({dbname, name})
  end

  def create_bucket(kdb = %Kdb{db: db, buckets: buckets}, mod) do
    name = mod.name()
    {:ok, handle} = :rocksdb.create_column_family(db, to_charlist(name), [])
    bucket = mod.new(kdb.name, db, handle)
    new_kdb = %{kdb | buckets: Map.put(buckets, name, bucket)}
    Kdb.Registry.register(name, new_kdb)
    new_kdb
  end

  def drop_bucket(kdb = %Kdb{db: db, name: dbname, buckets: buckets}, name) do
    handle = Map.get(buckets, name)
    :rocksdb.drop_column_family(db, handle)
    Kdb.Registry.unregister({dbname, name})
    %{kdb | buckets: Map.delete(buckets, name)}
  end

  def new_batch(kdb) do
    {:ok, batch} = :rocksdb.batch()
    %{kdb | batch: batch}
  end

  @spec batch(name :: atom() | String.t()) :: batch :: reference()
  def batch(name) do
    key = {:batch, name}

    case Kdb.Registry.lookup(key) do
      [{_, batch}] ->
        batch

      _ ->
        {:ok, batch} = :rocksdb.batch()
        Kdb.Registry.register(key, batch)
        batch
    end
  end

  @spec batch(Kdb.t(), name :: atom() | String.t()) :: Kdb.t()
  def batch(kdb, name) do
    batch = batch(name)
    put_batch(kdb, batch)
  end

  @spec transaction(bucket :: Kdb.t(), fun :: (Kdb.t() -> any())) :: :ok | {:error, any()}
  def transaction(%Kdb{db: db, buckets: buckets} = kdb, fun) do
    {:ok, batch} = :rocksdb.batch()

    kdb = %{
      kdb
      | batch: batch,
        buckets:
          Map.new(buckets, fn {name, bucket = %{module: module}} ->
            {name, %{bucket | batch: batch, t: module.new_table(), cachable: false}}
          end)
    }

    result =
      try do
        fun.(kdb)
        :rocksdb.write_batch(db, batch, [])
      catch
        _exit, reason ->
          {:error, reason}
      end

    # delete ets tables
    for {_, bucket} <- kdb.buckets do
      :ets.delete(bucket.t)
    end

    :rocksdb.release_batch(batch)

    result
  end

  def begin_transaction(kdb = %Kdb{buckets: buckets}) do
    {:ok, batch} = :rocksdb.batch()

    %{
      kdb
      | batch: batch,
        buckets:
          Map.new(buckets, fn {name, bucket = %{module: module}} ->
            {name, %{bucket | batch: batch, t: module.new_table(), cachable: false}}
          end)
    }
  end

  def commit_transaction(kdb = %Kdb{db: db, batch: batch}) do
    :rocksdb.write_batch(db, batch, [])
    # delete ets tables
    for {_, bucket} <- kdb.buckets do
      :ets.delete(bucket.t)
    end

    :rocksdb.release_batch(batch)
  end

  def key_merge(keys) do
    Enum.join(keys, ":")
  end

  def key_merge(key1, key2) do
    <<key1::binary, ":", key2::binary>>
  end

  defp put_batch(%Kdb{buckets: buckets} = kdb, batch) do
    %{
      kdb
      | batch: batch,
        buckets: Map.new(buckets, fn {name, bucket} -> {name, %{bucket | batch: batch}} end)
    }
  end

  defp put_batch(kdb, batch) do
    %{kdb | batch: batch}
  end

  # def has_key?(%Kdb{db: db, tables: tables}, name, key) do
  #   %{ets: ets, handle: handle, exp: exp} = Map.get(tables, name)

  #   case :ets.member(ets, key) do
  #     true ->
  #       true

  #     false ->
  #       case :rocksdb.get(db, handle, key, []) do
  #         :not_found ->
  #           false

  #         {:ok, value} ->
  #           result = binary_to_term(value)
  #           :ets.insert(ets, {key, result})
  #           if exp, do: Cache.put(name, key)
  #           true

  #         err ->
  #           err
  #       end
  #   end
  # end

  @doc """
  Usage:
    tr = Kdb.get_tr(:blockchain)
    opts = [
      init: {:seek, "ac_"},
      direction: :next
    ]
    Kdb.foreach(tr, :accounts, fn key, value ->
      # do something with key and value
    end, opts)
  """

  # def foreach(%Kdb{db: db, tables: tables}, name, fun, opts \\ []) do
  #   %{handle: handle} = Map.get(tables, name)
  #   # seek: <<>> | :last | binary()
  #   initial_seek = Keyword.get(opts, :seek, <<>>)
  #   # direction: :next | :prev
  #   direction = Keyword.get(opts, :direction, :next)

  #   {:ok, iter} = :rocksdb.iterator(db, handle, [])

  #   try do
  #     case :rocksdb.iterator_move(iter, initial_seek) do
  #       {:ok, key, value} ->
  #         fun.(key, binary_to_term(value))
  #         do_foreach(iter, fun, direction)

  #       _ ->
  #         :rocksdb.iterator_close(iter)
  #     end
  #   rescue
  #     e ->
  #       :rocksdb.iterator_close(iter)
  #       reraise e, __STACKTRACE__
  #   end
  # end

  # defp do_foreach(iter, fun, direction) do
  #   case :rocksdb.iterator_move(iter, direction) do
  #     {:ok, key, value} ->
  #       fun.(key, binary_to_term(value))
  #       do_foreach(iter, fun, direction)

  #     _ ->
  #       :rocksdb.iterator_close(iter)
  #   end
  # end

  # def while(%Kdb{db: db, tables: tables}, name, acc, fun, opts \\ []) do
  #   %{handle: handle} = Map.get(tables, name)
  #   initial_seek = Keyword.get(opts, :seek, <<>>)
  #   direction = Keyword.get(opts, :direction, :next)

  #   {:ok, iter} = :rocksdb.iterator(db, handle, [])

  #   try do
  #     case :rocksdb.iterator_move(iter, initial_seek) do
  #       {:ok, key, value} ->
  #         {action, result} = fun.({key, binary_to_term(value)}, acc)

  #         if action == :cont do
  #           do_while(iter, acc, fun, direction)
  #         else
  #           :rocksdb.iterator_close(iter)
  #           result
  #         end

  #       _ ->
  #         :rocksdb.iterator_close(iter)
  #     end
  #   rescue
  #     e ->
  #       :rocksdb.iterator_close(iter)
  #       reraise e, __STACKTRACE__
  #   end
  # end

  # defp do_while(iter, acc, fun, direction) do
  #   case :rocksdb.iterator_move(iter, direction) do
  #     {:ok, key, value} ->
  #       {action, result} = fun.({key, binary_to_term(value)}, acc)

  #       if action == :cont do
  #         do_while(iter, acc, fun, direction)
  #       else
  #         :rocksdb.iterator_close(iter)
  #         result
  #       end

  #     _ ->
  #       :rocksdb.iterator_close(iter)
  #   end
  # end

  # def fold(%Kdb{db: db, tables: tables}, name, fun, acc, opts \\ []) do
  #   %{handle: handle} = Map.get(tables, name)
  #   initial_seek = Keyword.get(opts, :seek, <<>>)
  #   direction = Keyword.get(opts, :direction, :next)

  #   {:ok, iter} = :rocksdb.iterator(db, handle, [])

  #   try do
  #     case :rocksdb.iterator_move(iter, initial_seek) do
  #       {:ok, key, value} ->
  #         acc = fun.(key, binary_to_term(value), acc)
  #         do_fold(iter, fun, acc, direction)

  #       _ ->
  #         :rocksdb.iterator_close(iter)
  #         acc
  #     end
  #   rescue
  #     e ->
  #       :rocksdb.iterator_close(iter)
  #       reraise e, __STACKTRACE__
  #   end
  # end

  # defp do_fold(iter, fun, acc, direction) do
  #   case :rocksdb.iterator_move(iter, direction) do
  #     {:ok, key, value} ->
  #       acc = fun.(key, binary_to_term(value), acc)
  #       do_fold(iter, fun, acc, direction)

  #     _ ->
  #       :rocksdb.iterator_close(iter)
  #       acc
  #   end
  # end

  # def put(%Kdb{batch: batch, tables: tables}, name, key, value) do
  #   case Map.get(tables, name) do
  #     %{handle: handle, ets: ets, exp: false} ->
  #       :ets.insert(ets, {key, value})
  #       :rocksdb.batch_put(batch, handle, key, term_to_binary(value))

  #     %{handle: handle, ets: ets} ->
  #       :ets.insert(ets, {key, value})
  #       :rocksdb.batch_put(batch, handle, key, term_to_binary(value))
  #       Cache.put(name, key)
  #   end
  # end

  # def put_db(%Kdb{batch: batch, tables: tables}, name, key, value) do
  #   %{handle: handle} = Map.get(tables, name)
  #   :rocksdb.batch_put(batch, handle, key, term_to_binary(value))
  # end

  # def get(tr = %Kdb{tables: tables}, name, key) do
  #   %{ets: ets} = Map.get(tables, name)

  #   case :ets.lookup(ets, key) do
  #     [{^key, value}] -> value
  #     [] -> get_from_db(tr, name, key)
  #   end
  # end

  # def fetch(tr = %Kdb{tables: tables}, name, key) do
  #   %{ets: ets} = Map.get(tables, name)

  #   case :ets.lookup(ets, key) do
  #     [{^key, value}] -> {:ok, value}
  #     [] -> fetch_from_db(tr, name, key)
  #   end
  # end

  # def slot(%Kdb{tables: tables}, name, position) do
  #   %{ets: ets} = Map.get(tables, name)
  #   :ets.slot(ets, position)
  # end

  # defp load_from_db(tr, ets, name, key) do
  #   if not :ets.member(ets, key) do
  #     case fetch_from_db(tr, name, key) do
  #       {:ok, value} -> :ets.insert(ets, {key, value})
  #       _ -> false
  #     end
  #   end
  # end

  # def incr(tr = %Kdb{batch: batch, tables: tables}, name, key, {elem, amount}) do
  #   %{handle: handle, ets: ets} = Map.get(tables, name)

  #   load_from_db(tr, ets, name, key)

  #   result = :ets.update_counter(ets, key, {elem, amount}, {key, 0})
  #   :rocksdb.batch_put(batch, handle, key, term_to_binary(result))

  #   # :rocksdb.batch_merge(batch, key, term_to_binary({:int_add, amount}), [])
  #   result
  # end

  # def incr_non_zero(tr = %Kdb{batch: batch, tables: tables}, name, key, {elem, neg_amount}) do
  #   %{handle: handle, ets: ets} = Map.get(tables, name)

  #   load_from_db(tr, ets, name, key)

  #   case :ets.update_counter(ets, key, {elem, neg_amount}, {key, 0}) do
  #     result when 0 > result ->
  #       :ets.update_counter(ets, key, {elem, abs(neg_amount)})
  #       {:error, "Insufficient balance"}

  #     result ->
  #       :rocksdb.batch_put(batch, handle, key, term_to_binary(result))
  #       # :rocksdb.batch_merge(batch, handle, key, term_to_binary({:int_add, neg_amount}))
  #       {:ok, result}
  #   end
  # end

  # def incr_limit(tr = %Kdb{batch: batch, tables: tables}, name, key, {elem, amount}, limit) do
  #   %{handle: handle, ets: ets} = Map.get(tables, name)

  #   load_from_db(tr, ets, name, key)

  #   case :ets.update_counter(ets, key, {elem, amount}, {key, 0}) do
  #     result when limit != 0 and result > limit ->
  #       :ets.update_counter(ets, key, {elem, -amount})
  #       {:error, "Limit exceeded"}

  #     result ->
  #       :rocksdb.batch_put(batch, handle, key, term_to_binary(result))
  #       # :rocksdb.batch_merge(batch, handle, key, term_to_binary({:int_add, amount}))
  #       {:ok, result}
  #   end
  # end

  # def total(tr, name) do
  #   case fetch(tr, name, @stat_count) do
  #     {:ok, count} -> count
  #     _ -> 0
  #   end
  # end

  # def count_one(tr, name) do
  #   incr(tr, name, @stat_count, {2, 1})
  # end

  # def discount_one(tr, name) do
  #   incr(tr, name, @stat_count, {2, -1})
  # end

  # def ets_total(%Kdb{tables: tables}, name) do
  #   %{ets: ets} = Map.get(tables, name)
  #   :ets.info(ets, :size)
  # end

  # def delete(%Kdb{batch: batch, tables: tables}, name, key) do
  #   %{handle: handle, ets: ets, exp: exp} = Map.get(tables, name)
  #   :ets.delete(ets, key)
  #   if exp, do: Cache.remove(key)
  #   :rocksdb.batch_delete(batch, handle, key)
  # end

  # def delete_db(%Kdb{batch: batch, tables: tables}, name, key) do
  #   %{handle: handle} = Map.get(tables, name)
  #   :rocksdb.batch_delete(batch, handle, key)
  # end

  # def get_from_db(%Kdb{db: db, tables: tables}, name, key) do
  #   %{handle: handle, ets: ets, exp: exp} = Map.get(tables, name)

  #   case :rocksdb.get(db, handle, key, []) do
  #     {:ok, value} ->
  #       result = binary_to_term(value)
  #       if exp, do: Cache.put(name, key)
  #       :ets.insert(ets, {key, result})
  #       result

  #     _err ->
  #       nil
  #   end
  # end

  # def fetch_from_db(%Kdb{db: db, tables: tables}, name, key) do
  #   %{handle: handle, ets: ets, exp: exp} = Map.get(tables, name)

  #   case :rocksdb.get(db, handle, key, []) do
  #     {:ok, value} ->
  #       result = binary_to_term(value)
  #       if exp, do: Cache.put(name, key)
  #       :ets.insert(ets, {key, result})
  #       {:ok, result}

  #     err ->
  #       err
  #   end
  # end

  def batch_save(%{batch: batch}, filename) do
    binary = :rocksdb.batch_tolist(batch) |> term_to_binary()
    File.write(filename, binary)
  end

  def batch_load(%{buckets: buckets}, dbfile, filename) do
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

  def commit(%{db: db}, batch) when is_reference(batch) do
    if :rocksdb.batch_count(batch) > 0 do
      :rocksdb.write_batch(db, batch, [])
    end

    :rocksdb.release_batch(batch)

    :ok
  end

  def commit(%{db: db}, name) do
    key = {:batch, name}

    case Kdb.Registry.lookup(key) do
      {:ok, batch} ->
        if is_reference(batch) do
          if :rocksdb.batch_count(batch) > 0 do
            :rocksdb.write_batch(db, batch, [])
          end

          :rocksdb.release_batch(batch)
        end

        Kdb.Registry.unregister(key)

      _ ->
        nil
    end

    :ok
  end

  # def load_all(tr = %Kdb{tables: tables}, name) do
  #   %{ets: ets} = Map.get(tables, name)

  #   foreach(tr, name, fn key, value ->
  #     :ets.insert(ets, {key, value})
  #   end)
  # end

  # def savepoint(%{batch: batch}) do
  #   :rocksdb.batch_savepoint(batch)
  # end

  @spec snapshot(t()) :: no_return()
  def snapshot(%{db: db} = object) do
    {:ok, snapshot} = :rocksdb.snapshot(db)
    %{object | db: snapshot}
  end

  @spec release_snapshot(t() | reference()) :: no_return()
  def release_snapshot(%__MODULE__{db: db}) do
    :rocksdb.release_snapshot(db)
  end

  def release_snapshot(snapshot) when is_reference(snapshot) do
    :rocksdb.release_snapshot(snapshot)
  end

  def release_batch(%{batch: batch}) when is_reference(batch) do
    :rocksdb.release_batch(batch)
  end

  def release_batch(name) when is_binary(name) or is_atom(name) do
    key = {:batch, name}

    case Kdb.Registry.lookup(key) do
      {:ok, batch} ->
        if is_reference(batch) do
          :rocksdb.release_batch(batch)
        end

        Kdb.Registry.unregister(key)

      _ ->
        nil
    end

    :ok
  end

  def release_batch(_batch), do: :ok

  @spec restore(charlist(), charlist()) :: :ok | {:error, term()}
  def restore(target, output) do
    zip_file = IO.iodata_to_binary([target, ".zip"]) |> to_charlist()

    case ZipUtil.extract(zip_file, target) do
      {:ok, _} ->
        case :rocksdb.open_backup_engine(target) do
          {:ok, ref} ->
            case :rocksdb.restore_db_from_latest_backup(ref, output) do
              :ok ->
                :rocksdb.close_backup_engine(ref)

              {:error, _reason} = err ->
                err
            end

          {:error, _reason} = err ->
            err
        end

      {:error, _reason} = err ->
        err
    end
  end

  @spec backup(t(), charlist() | binary()) :: :ok | {:error, term()}
  def backup(%Kdb{db: db}, target) do
    case :rocksdb.open_backup_engine(target) do
      {:ok, ref} ->
        case :rocksdb.create_new_backup(ref, db) do
          :ok ->
            :rocksdb.close_backup_engine(ref)
            zip_file = IO.iodata_to_binary([target, ".zip"])

            case ZipUtil.compress_folder(target, zip_file) do
              {:ok, _} ->
                File.rm_rf!(target)
                :ok

              {:error, _reason} = err ->
                err
            end

          {:error, _reason} = err ->
            err
        end

      {:error, _reason} = err ->
        err
    end
  end

  def close(%Kdb{name: name, batch: batch, db: db, buckets: buckets}) do
    if is_reference(batch) do
      :rocksdb.release_batch(batch)
    end

    for {_, %{t: t}} <- buckets do
      :ets.delete(t)
    end

    :persistent_term.erase({@key, name})
    :rocksdb.close(db)
  end

  def destroy(%Kdb{name: name, buckets: buckets, folder: folder}) do
    :rocksdb.destroy(folder, [])

    for {_, %{t: t}} <- buckets do
      try do
        :ets.delete(t)
      catch
        _, _ -> nil
      end
    end

    :persistent_term.erase({@key, name})
  end

  def term_to_binary(term) do
    :erlang.term_to_binary(term)
  end

  def binary_to_term(binary) do
    :erlang.binary_to_term(binary, [:safe])
  end
end
