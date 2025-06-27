defmodule Kdb.Scheduler do
  @cleanup_interval :timer.minutes(15)

  use Poolder.Scheduler,
    hibernate_after: :timer.seconds(60),
    jobs: [
      {:cleanup, @cleanup_interval}
    ]

  def cleanup(_args) do
    # name = Keyword.fetch!(args.opts, :name)
    time = :os.system_time(:milliseconds)
    total = Kdb.Cache.cleanup(time)
    IO.puts("Cleaned up #{total} entries")

    :ok
  end
end
