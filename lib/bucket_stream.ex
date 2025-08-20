defmodule Kdb.Bucket.Stream do
  def stream(%Kdb.Bucket{dbname: dbname, handle: handle, module: module}, opts \\ []) do
    kdb = Kdb.get(dbname)
    db = kdb.store
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
