# Kdb
![Version](https://img.shields.io/badge/version-0.1.5-blue.svg)
![Status](https://img.shields.io/badge/status-active-green.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

KDB is a real-time database that combines RocksDB as a persistent backend, ETS as an in-memory cache with TTL support, and SQLite for secondary indexing. It supports parallel writes for high-throughput ingestion, enabling fast reads, durable storage, and efficient lookups in demanding environments

## Key Features
- Fast, TTL-enabled ETS layer for short-lived, memory-resident data.
- Reliable RocksDB persistence with efficient key-value write performance.
- Unified read/write interface with cache-first logic and optional async sync to disk.
- Configurable batching and write coalescing for high-throughput pipelines.
- Suitable for event processors, caching proxies, ephemeral data stores, and real-time systems under sustained load.
- Support unique and secundary indexes with SQLite

>Tested exclusively on Linux x86_64 systems (Ubuntu 22.04.3 LTS)

> Note: This library is under active development and evolving rapidly, with the goal of reaching a stable release at version v0.2.0. Contributions, testing, and feedback are welcome as the project matures toward production readiness

> ⚠️ **Build requirement**: Compiling RocksDB requires **CMake version 3.4 or higher**. Please ensure it is installed and available in your system path.

## Installation

The package can be installed by adding `kdb` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:kdb, "~> 0.1.5"}
  ]
end
```

> Note: `cmake>=3.4` is required to compile RocksDB from source.

## Roadmap
- [✅] Start
- [✅] Get & put
- [✅] Fetch & Access
- [✅] Batching
- [✅] TTL cache
- [✅] Count keys globally and by pattern
- [✅] Encoding/Decoding
- [✅] Enumerable & Stream
- [✅] Add and remove list items
- [✅] Increment
- [✅] Decrement
- [✅] Delete
- [✅] Transactions
- [✅] Close
- [✅] Backup & Restore
- [✅] Testing
- [✅] Indexing (Unique & Secondary)
- [❌] Sharding
- [❌] Replication
- [❌] Benchmarking
- [❌] Documentation

> Relevant: Key counting works only for put_new/3 and delete/2

## Usage
```elixir
# Create a bucket
defmodule MyBucket do
  use Kdb.Bucket, 
  # Use atom, default current module last name (it is transformed to atom)
  name: :my_bucket,
  # no stats
  stats: false,
  # default is 5 minutes (300_000)
  ttl: 30_000
end

defmodule AccountIndex do
  use Kdb.Bucket, name: :accIndex, ttl: 60_000
end

defmodule Accounts do
  use Kdb.Bucket, 
  # Use atom, default current module last name (it is transformed to atom)
  name: :account,
  # Unique indexes
  unique: [{:name, AccountIndex}],
  # Secondary indexes
  secondary: [:age],
  # count diferents keys with regex
  match_count: [
    # name | patterm | custom function (name, patterm)
    {"admins", "admin_", &String.starts_with?/2},
    {"users", "user_", &String.starts_with?/2}
  ],
  # TTL in cache (5 minutes)
  ttl: 30_000
end

# Start the Kdb database
{:ok, _sup} = Kdb.start_link(name: :dbname, folder: "database", buckets: [MyBucket, AccountIndex, Accounts])

# Get the database instance
kdb = Kdb.get(:dbname)
# Start/Get batch
batch = Kdb.Batch.new(name: :first, db: kdb)

# Write data (batch is necessary for transactional operations)
MyBucket.put(batch, "key", "value")
MyBucket.put(batch, "mykey", 10)
MyBucket.put(batch, "mymap", %{a: 1, b: 2, c: 3})
MyBucket.put(batch, "mytuple", {3, 2, 1, :go})
MyBucket.incr(batch, "mykey", 12)
MyBucket.incr(batch, "mykey", -2)
Accounts.put_new(batch, "user_sdlkfjk", %{name: "Jules", age: 19})
Accounts.put_new(batch, "user_sdoljf", %{name: "Markus", age: 37})
Accounts.put_new(batch, "admin_jsdhju", %{name: "Gary", age: 29})
# Delete data
MyBucket.delete(batch, "key")

# Commit changes from first batch
:ok = Kdb.Batch.commit(batch)

# Read data
myb = Kdb.get_bucket(kdb, :my_bucket)
accounts = Kdb.get_bucket(kdb, :account)
accounts["jules_id"] |> IO.inspect(label: "Jules account:")
myb["mykey"] == 10

# Transactional operations (memory isolation)
Kdb.transaction(kdb, fn batch ->
  MyBucket.put(batch, "jess", 700)
  MyBucket.incr(batch, "carlos", 1000)
  MyBucket.put(batch, "jim", 950)
  MyBucket.put(batch, "caroline", 100)
  MyBucket.put(batch, "jony", 500)
end)

# List data
myb |> Enum.to_list() |> IO.inspect()

# Stream data
accounts |> Kdb.Bucket.Stream.stream() |> Enum.to_list() |> IO.inspect()

# Close the database
:ok = Kdb.close(kdb)
```

## Configuration
```elixir
# config/config.exs
config :myapp, :kdb,
  name: :dbname,
  folder: "database",
  buckets: [MyBucket]
```

```elixir
# lib/application.ex
defmodule MyApp do
  use Application

  def start(_type, _args) do
    children = [
      {Kdb, Application.get_env(:myapp, :kdb)}
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

## Testing
```bash
mix test test/ttl_test.exs
mix test test/batch_test.exs
mix test test/tx_test.exs
mix test test/incr_test.exs
mix test test/index_test.exs
mix test test/backup_test.exs
mix test test/destroy_test.exs
mix test test/restore_test.exs
mix test test/inspect_test.exs
mix test test/clean_test.exs
```

## License
This library is licensed under the MIT License.

