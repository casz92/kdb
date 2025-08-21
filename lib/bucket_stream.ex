defmodule Kdb.Bucket.Stream do
  def stream(%Kdb.Bucket{dbname: dbname, handle: handle, module: module}, opts \\ []) do
    kdb = Kdb.get(dbname)
    db = kdb.store
    # seek: <<>> | <<0>> | :last
    initial_seek = Keyword.get(opts, :seek, <<>>)
    # action: :next | :prev
    action = Keyword.get(opts, :action, :next)
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
          :rocksdb.iterator_close(iter)
          {:halt, iter}

        {:ok, iter, key, value} ->
          item = {key, decoder_fun.(value)}

          next =
            case :rocksdb.iterator_move(iter, action) do
              {:ok, next_key, next_val} -> {:ok, iter, next_key, next_val}
              _ -> {:done, iter}
            end

          {[item], next}
      end,

      # After: close iterator
      fn _iter ->
        :ok
      end
    )
  end

  def keys(%Kdb.Bucket{dbname: dbname, handle: handle}, opts \\ []) do
    kdb = Kdb.get(dbname)
    db = kdb.store
    # seek: <<>> | <<0>> | :last
    initial_seek = Keyword.get(opts, :seek, <<>>)
    # action: :next | :prev
    action = Keyword.get(opts, :action, :next)

    Stream.resource(
      # Start: open iterator and seek
      fn ->
        {:ok, iter} = :rocksdb.iterator(db, handle, [])

        state =
          case :rocksdb.iterator_move(iter, initial_seek) do
            {:ok, key, _value} -> {:ok, iter, key}
            _ -> {:done, iter}
          end

        state
      end,

      # Next: return {k, v} and move iterator
      fn
        {:done, iter} ->
          :rocksdb.iterator_close(iter)
          {:halt, iter}

        {:ok, iter, key} ->
          next =
            case :rocksdb.iterator_move(iter, action) do
              {:ok, next_key, _next_val} -> {:ok, iter, next_key}
              _ -> {:done, iter}
            end

          {[key], next}
      end,

      # After: close iterator
      fn _iter ->
        :ok
      end
    )
  end
end
