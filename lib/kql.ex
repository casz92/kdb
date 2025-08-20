defmodule Kql do
  def to_sql(opts) when is_list(opts) do
    {sql, params} =
      Enum.reduce(opts, {[], []}, fn item, {acc_sql, acc_params} ->
        clause_to_sql(item, acc_sql, acc_params)
      end)

    sql =
      sql
      |> Enum.reverse()
      |> Enum.join(" ")

    params = Enum.reverse(params)

    %{sql: sql, params: params}
  end

  defp clause_to_sql([], acc_sql, acc_params) do
    {acc_sql, acc_params}
  end

  defp clause_to_sql([item | rest], acc_sql, acc_params) do
    {acc_sql, acc_params} = clause_to_sql(item, acc_sql, acc_params)
    clause_to_sql(rest, acc_sql, acc_params)
  end

  defp clause_to_sql({key, list}, acc_sql, acc_params) when is_list(list) do
    {acc_sql, acc_params} = clause_to_sql(list, acc_sql, acc_params)
    {["#{key}" | acc_sql], acc_params}
  end

  defp clause_to_sql({key, text}, acc_sql, acc_params) when is_binary(text) do
    {[text, "#{key}"] ++ acc_sql, acc_params}
  end

  defp clause_to_sql({key, value}, acc_sql, acc_params) do
    {["?", "#{key}"] ++ acc_sql, [value | acc_params]}
  end

  defp clause_to_sql({key, oper, value}, acc_sql, acc_params) do
    {["?", oper, "#{key}"] ++ acc_sql, [value | acc_params]}
  end

  defp clause_to_sql({key, oper, value, after_value}, acc_sql, acc_params) do
    {[after_value, "?", oper, "#{key}"] ++ acc_sql, [value | acc_params]}
  end

  defp clause_to_sql(key, acc_sql, acc_params) do
    {["#{key}" | acc_sql], acc_params}
  end
end
