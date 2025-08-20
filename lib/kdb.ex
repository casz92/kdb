defmodule Kdb do
  @type tname :: atom() | binary()

  @type t :: %__MODULE__{
          name: tname(),
          # rocksdb database handle
          store: reference() | nil,
          # sqlite connection
          indexer: reference() | nil,
          folder: charlist(),
          buckets: %{tname() => Kdb.Bucket.t()}
        }

  defstruct [:name, :store, :indexer, :folder, :buckets]
  @default_cfs ~c"default"

  @open_options [
    create_if_missing: true,
    merge_operator: :erlang_merge_operator
  ]

  @compile {:inline, get: 1, get_bucket: 2}

  alias __MODULE__

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
  @spec new(otps :: keyword()) :: t()
  def new(opts) do
    dbname = Keyword.fetch!(opts, :name)
    root = Keyword.fetch!(opts, :folder)
    File.mkdir(root)
    modules = Keyword.get(opts, :buckets, [])
    folder = Path.join(root, "data") |> to_charlist()
    conn = Kdb.Indexer.new(opts)

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
    default_bucket_opts = [
      dbname: dbname,
      handle: default_cf
    ]

    default_bucket = DefaultBucket.new(default_bucket_opts)

    # load buckets
    buckets =
      Enum.zip(modules, cfs)
      |> Enum.map(fn {mod, handle} ->
        bucket_opts = [
          dbname: dbname,
          handle: handle
        ]

        bucket = mod.new(bucket_opts)
        {bucket.name, bucket}
      end)
      |> Map.new()
      |> Map.put(default_bucket.name, default_bucket)

    kdb = %__MODULE__{name: dbname, store: db, indexer: conn, folder: root, buckets: buckets}
    # register kdb
    Kdb.Registry.register(kdb)
    kdb
  end

  def get(name) do
    Kdb.Registry.get_db(name)
  end

  def get_bucket(%Kdb{buckets: buckets}, name) do
    Map.get(buckets, name)
  end

  @spec transaction(t(), (Kdb.Batch.t() -> any())) :: :ok | {:error, term()}
  def transaction(kdb, fun) do
    batch = Kdb.Batch.new(name: make_ref(), db: kdb)

    result =
      try do
        fun.(batch)
        :ok = Kdb.Batch.commit(batch)
      catch
        _exit, reason ->
          Kdb.Batch.release(batch)
          {:error, reason}
      end

    result
  end

  def close(%Kdb{store: db, indexer: indexer} = kdb) do
    Kdb.Registry.unregister(kdb)

    try do
      :ok = :rocksdb.close(db)
      :ok = Kdb.Indexer.close(indexer)
      :ok
    rescue
      e ->
        {:error, e}
    end
  end

  def destroy(%Kdb{folder: folder} = kdb) do
    Kdb.Registry.unregister(kdb)

    try do
      close(kdb)
      File.rm_rf!(folder)
      :ok
    rescue
      e ->
        {:error, e}
    end
  end

  @spec backup(t(), charlist()) :: :ok | {:error, term()}
  def backup(%Kdb{store: db}, target) do
    {:ok, ref} = :rocksdb.open_backup_engine(target)

    try do
      :ok = :rocksdb.create_new_backup(ref, db)
    rescue
      e ->
        IO.inspect(e, label: "Backup error")
        {:error, e}
    after
      :rocksdb.close_backup_engine(ref)
    end
  end

  @spec restore(charlist(), charlist()) :: :ok | {:error, term()}
  def restore(source, folder_destiny) do
    {:ok, ref} = :rocksdb.open_backup_engine(source)
    :ok = File.mkdir_p(folder_destiny)
    :ok = File.mkdir(folder_destiny)

    try do
      {:ok, backups} = :rocksdb.get_backup_info(ref)

      if backups == [] do
        {:error, :no_backups}
      else
        backup = List.first(backups)
        :ok = :rocksdb.restore_db_from_backup(ref, backup.id, folder_destiny)
        :ok
      end
    rescue
      e ->
        IO.inspect(e, label: "Restore error")
        {:error, e}
    after
      :rocksdb.close_backup_engine(ref)
    end
  end
end
