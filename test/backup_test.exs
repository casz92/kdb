# mix test test/backup_test.exs
defmodule BackupTest do
  use ExUnit.Case, async: false

  import KdbTestUtils

  setup_all do
    open()
  end

  test "backup", %{kdb: kdb} = params do
    case Kdb.backup(kdb, "backup") do
      :ok ->
        IO.inspect("Backup completed successfully", label: "Backup Status")

      {:error, :target_exists} ->
        IO.inspect("Backup failed: Target already exists", label: "Backup Status")
    end

    close(params)
  end
end
