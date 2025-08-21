defmodule Kdb.MixProject do
  use Mix.Project

  @version "0.1.4"

  def project do
    [
      app: :kdb,
      name: "Kdb",
      description:
        "High performance realtime database combining ETS with TTL for caching, RocksDB for persistent parallel writes, and SQLite for secondary indexing and fast lookups",
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package()
    ]
  end

  defp package do
    [
      maintainers: ["Carlos Suarez"],
      files: ~w(lib .formatter.exs mix.exs README* LICENSE*),
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/casz92/kdb"}
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    []
  end

  defp deps do
    [
      {:rocksdb, "~> 1.9"},
      {:exqlite, "0.33.0"},
      {:poolder, "~> 0.1.11"},
      {:ex_doc, "~> 0.29.4", only: :dev, runtime: false}
    ]
  end
end
