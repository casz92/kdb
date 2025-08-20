ExUnit.start()

defmodule Bucket do
  use Kdb.Bucket, name: :bucket, ttl: 1_000
end

defmodule Bucket2Index do
  use Kdb.Bucket, name: :bucket2index, ttl: 60_000
end

defmodule Bucket2 do
  use Kdb.Bucket,
    name: Bucket2,
    unique: [{:name, Bucket2Index}],
    secondary: [:age],
    ttl: 1_000
end

defmodule KdbTestUtils do
  def open do
    opts = [name: :dbname, folder: "database", buckets: [Bucket, Bucket2Index, Bucket2]]
    {:ok, sup} = Kdb.start_link(opts)
    kdb = Kdb.get(:dbname)

    %{kdb: kdb, sup: sup}
  end

  def close(%{kdb: kdb, sup: sup}) do
    :ok = Kdb.close(kdb)
    :ok = Supervisor.stop(sup)
  end
end
