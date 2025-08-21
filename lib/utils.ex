defmodule Kdb.Utils do
  @compile {:inline,
            [
              key_merge: 1,
              key_merge: 2,
              binary_to_term: 1,
              term_to_binary: 1
            ]}

  def key_merge(keys) do
    Enum.join(keys, ":")
  end

  def key_merge(key1, key2) do
    <<key1::binary, ":", key2::binary>>
  end

  def term_to_binary(term) do
    :erlang.term_to_binary(term)
  end

  def binary_to_term(binary) do
    :erlang.binary_to_term(binary, [:safe])
  end

  def to_bucket_name(mod) do
    mod
    |> to_string()
    |> String.split(".")
    |> List.last()
    # |> String.downcase()
    |> String.to_atom()
  end
end
