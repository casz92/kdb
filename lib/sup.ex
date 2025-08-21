defmodule Kdb.Supervisor do
  use Supervisor

  def start_link(args \\ []) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(args) do
    name = Keyword.fetch!(args, :name)
    Kdb.new(args)

    children = [
      Kdb.Registry,
      {Kdb.Scheduler, [name: name]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
