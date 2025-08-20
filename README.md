# Kdb
![Version](https://img.shields.io/badge/version-0.1.1-blue.svg)
![Status](https://img.shields.io/badge/status-non--stable-red.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

This library integrates RocksDB as a persistent storage backend with ETS as an in-memory cache featuring configurable TTL (time-to-live) support. It is purpose-built for high-demand environments that require both rapid read performance and robust, fault-tolerant write operations.
By leveraging ETS for ultra-low-latency access and utilizing RocksDB for durable, disk-based storage, the system achieves a finely balanced architecture that handles heavy write loads without compromising speed or data integrity.

## Key Features
- Fast, TTL-enabled ETS layer for short-lived, memory-resident data.
- Reliable RocksDB persistence with efficient key-value write performance.
- Unified read/write interface with cache-first logic and optional async sync to disk.
- Configurable batching and write coalescing for high-throughput pipelines.
- Suitable for event processors, caching proxies, ephemeral data stores, and real-time systems under sustained load.

>Tested exclusively on Linux x86_64 systems (Ubuntu 22.04.3 LTS)

> Note: This library is under active development and evolving rapidly, with the goal of reaching a stable release at version v0.2.0. Contributions, testing, and feedback are welcome as the project matures toward production readiness

> ⚠️ **Build requirement**: Compiling RocksDB requires **CMake version 3.4 or higher**. Please ensure it is installed and available in your system path.


## Installation

The package can be installed by adding `kdb` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:kdb, "~> 0.1.1"}
  ]
end
```

> Note: `cmake>=3.4` is required to compile RocksDB from source.

## Roadmap
- [✅] Start
- [✅] Get & put
- [✅] Fetch & Access
- [✅] Batching
- [✅] TTL
- [❌] Count keys
- [✅] Encoding/Decoding
- [✅] Enumerable & Stream
- [✅] Increment
- [✅] Decrement
- [✅] Delete
- [✅] Transactions
- [✅] Close
- [❌] Backup & Restore
- [✍️] Testing
- [❌] Indexing
- [❌] Sharding
- [❌] Replication
- [❌] Benchmarking
- [❌] Documentation

## Usage
```elixir
# Create a bucket
defmodule MyBucket do
  use Kdb.Bucket, 
  # Use atom, default current module last name (it is transformed to atom)
  name: :my_bucket,
  # default is 5 minutes (300_000)
  ttl: 30_000
end

defmodule MyAccount do
  use Kdb.Bucket, 
  # Use atom, default current module last name (it is transformed to atom)
  name: :my_bucket,
  # Unique keys
  unique: ["name"],
  # secondary indexes
  secondary: ["age"],
  # default is 5 minutes (300_000)
  ttl: 30_000
end

# Start the Kdb database
{:ok, _sup} = Kdb.start_link(name: :dbname, folder: "database", buckets: [MyBucket])

# Get the database instance
kdb = Kdb.get(:dbname)
# Start/Get batch
kdb = Kdb.batch(kdb, "first batch")
# Get the bucket instance and the batch loaded inside
myb = Kdb.get_bucket(kdb, :my_bucket)

# Write data (batch is necessary for transactional operations)
MyBucket.put(myb, "key", "value")
MyBucket.put(myb, "mykey", 10)
MyBucket.put(myb, "mymap", %{a: 1, b: 2, c: 3})
MyBucket.put(myb, "mytuple", {3, 2, 1, :go})
MyBucket.incr(myb, "mykey", 12)
MyBucket.incr(myb, "mykey", -2)

# Read data
{:ok, "value"} = MyBucket.fetch(myb, "key")
myb["mykey"] == 20
"value" == MyBucket.get(myb, "key")

# Delete data
:ok = MyBucket.delete(myb, "key")

# Commit changes from first batch
:ok = Kdb.commit(kdb, "first batch")

# Transactional operations (memory isolation)
Kdb.transaction(kdb, fn kdb ->
  myb = kdb.buckets.my_bucket
  Bucket.put(myb, "jess", 700)
  Bucket.incr(myb, "carlos", 1000)
  Bucket.put(myb, "jim", 950)
  Bucket.put(myb, "caroline", 100)
  Bucket.put(myb, "jony", 500)
end)

# List data
myb |> Enum.to_list() |> IO.inspect()

# Stream data
myb |> Kdb.Stream.stream() |> Enum.to_list() |> IO.inspect()

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
mix test test/kdb_test.exs
```

## License
This library is licensed under the MIT License.

