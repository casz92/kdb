defmodule Kdb.Cache do
  @table_name __MODULE__
  @unit_time :millisecond
  # @cleanup_interval :timer.minutes(15)
  @expiration_time :timer.minutes(10)
  @dev Mix.env() == :dev

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

  @spec put(atom() | String.t(), binary()) :: boolean()
  def put(type, id) do
    timestamp = now() + @expiration_time
    :ets.insert(@table_name, {{id, type}, timestamp})
  end

  # @spec retrive_by_type(atom()) :: [binary() | String.t()]
  # def retrive_by_type(type) do
  #   # :ets.fun2ms(fn {{id, 1}, _readed_at} -> id end)
  #   match_spec =
  #     [{{{:"$1", type}, :"$2"}, [], [:"$1"]}]

  #   :ets.select(@table_name, match_spec)
  # end

  @spec delete(atom(), binary()) :: true
  def delete(type, id) do
    :ets.delete(@table_name, {id, type})
  end

  defp now do
    :os.system_time(@unit_time)
  end

  def cleanup(older_than) do
    n =
      :ets.foldl(
        fn {key, readed_at}, acc ->
          if readed_at < older_than do
            :ets.delete(@table_name, key)

            acc + 1
          else
            acc
          end
        end,
        0,
        @table_name
      )

    @dev and IO.puts("Deleted #{n} entries")

    n
  end
end
