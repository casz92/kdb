defmodule Kdb.Batch do
  defstruct [:name, :db, :store, :indexer, :cache, :tasker]

  @type t :: %__MODULE__{
          name: atom() | String.t(),
          db: Kdb.t(),
          store: Kdb.Store.Batch.t(),
          indexer: Kdb.Indexer.Batch.t(),
          cache: Kdb.Cache.t(),
          tasker: pid()
        }

  @default_batch :default

  def new(opts) do
    name = Keyword.get(opts, :name, @default_batch)

    case Kdb.Registry.get_batch(name) do
      nil ->
        kdb = Keyword.fetch!(opts, :db)
        public = Keyword.get(opts, :public, true)

        tasker_name =
          if is_reference(name) do
            :default_tasker
          else
            name
          end

        tasker =
          Keyword.get(opts, :tasker) ||
            Process.whereis(tasker_name) ||
            Poolder.Tasker.start_link(name: tasker_name, limit: 1) |> elem(1)

        cache_opts = Keyword.get(opts, :cache, [])

        batch =
          %__MODULE__{
            name: name,
            db: kdb,
            store: Kdb.Store.Batch.new(kdb.store),
            indexer: Kdb.Indexer.Batch.new(conn: kdb.indexer, tasker: tasker),
            cache:
              (cache_opts == [] and Kdb.Registry.get_cache(cache_opts)) ||
                Kdb.Cache.new(cache_opts),
            tasker: tasker
          }

        if public do
          Kdb.Registry.register(batch)
        end

        batch

      batch ->
        batch
    end
  end

  def commit(%__MODULE__{indexer: indexer, store: store, tasker: _tasker}) do
    :ok = Kdb.Store.Batch.commit(store)

    # [
    #   Poolder.Tasker.callback(tasker, fn ->
    :ok = Kdb.Indexer.Batch.commit(indexer)
    #   end)
    # ]
    # |> Poolder.Tasker.await(:infinity)

    :ok
  end

  def release(%__MODULE__{store: store, indexer: indexer, tasker: tasker} = batch) do
    Kdb.Store.Batch.release(store)
    Kdb.Indexer.Batch.release(indexer)
    Kdb.Registry.unregister(batch)
    :ok = Poolder.Tasker.stop(tasker)
  end
end
