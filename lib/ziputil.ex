defmodule ZipUtil do
  @spec compress_folder(charlist(), charlist()) :: :ok | {:error, term()}
  def compress_folder(folder_path, output_zip) do
    files = get_files(to_string(folder_path))
    :zip.create(output_zip, files)
  end

  @spec extract(charlist(), charlist()) :: :ok | {:error, term()}
  def extract(zip_file, target_folder) do
    :zip.extract(zip_file, [{:cwd, target_folder}])
  end

  defp get_files(folder_path) do
    Path.wildcard("#{folder_path}/**/*")
    |> Enum.map(&String.to_charlist(&1))
  end
end
