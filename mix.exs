defmodule Kdb.MixProject do
  use Mix.Project

  @version "0.1.1"

  def project do
    [
      app: :kdb,
      name: "Kdb",
      description:
        "High-performance caching layer using ETS with TTL over RocksDB for persistent, write-intensive, low-latency applications",
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
      # {:poolder, "~> 0.1.10"},
      {:poolder, path: "../poolder"},
      {:ex_doc, "~> 0.29.4", only: :dev, runtime: false}
    ]
  end
end
