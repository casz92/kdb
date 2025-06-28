defmodule Kdb.Scheduler do
  use Poolder.Scheduler,
    jobs: [
      {:cleanup, 300_000}
    ]

  def cleanup(_args) do
    time = :os.system_time(:millisecond)
    total = Kdb.Cache.cleanup(time)
    IO.puts("Cleaned up #{total} entries")

    :ok
  end
end
