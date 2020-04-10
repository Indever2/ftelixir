defmodule Ftelixir.Engine do
  use GenServer
  require Logger

  @index_table :index_table
  @disk_table :disk_table
  @keys_table :keys_table

  def init(options) do
    {:ok, disk_table} = :dets.open_file(@disk_table, [type: :set])
    :ets.new(@index_table, [:ordered_set, :protected, :named_table])
    :ets.from_dets(@index_table, disk_table)

    {:ok, _} = :dets.open_file(@keys_table, [type: :set])
    {:ok, options}
  end

  def start_link(_) do
    Logger.info("Starting the engine: #{inspect __MODULE__}")
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def handle_call({:add, key, index_list}, _from, state) do
    Enum.map(
      index_list,
      fn {word, coff} ->
        update_index(word, {key, coff})
      end
    )
    :ok = add_key_to_index(key)
    {:reply, :ok, state}
  end

  def handle_call({:get, word}, _from, state) do
    case :ets.lookup(@index_table, word) do
      [] -> {:reply, [], state}
      [{^word, matches}] ->
        existing_matches =
          Enum.reduce(matches, [], fn {key, coff}, acc ->
            if is_key_member?(key) do
              acc ++ [{key, coff}]
            else
              acc
            end
          end)

        if matches != existing_matches do
          if existing_matches != [] do
            replace_index(word, existing_matches)
          else
            true = :ets.delete(@index_table, word)
            :ok = :dets.delete(@disk_table, word)
          end
        end

        {:reply, [{word, existing_matches}], state}
    end
  end

  def handle_call(:all, _from, state) do
    res = :ets.match_object(@index_table, {:"$1", :"$2"})
    {:reply, res, state}
  end

  def handle_call({:delete, key}, _from, state) do
    delete_key_from_index(key)
    {:reply, :ok, state}
  end

  def handle_call(:purge, _from, state) do
    :ok = :dets.delete_all_objects(@disk_table)
    :ok = :dets.delete_all_objects(@keys_table)
    true = :ets.delete_all_objects(@index_table)

    {:reply, :purged, state}
  end

  def handle_call(:count, _from, state) do
    res =
      :ets.match_object(@index_table, {:"$1", :"$2"})
      |> Enum.map(fn {key, entries} -> {key, length(entries)} end)

    {:reply, res, state}
  end

  def terminate(_reason, _state) do
    Logger.info("#{inspect __MODULE__} is terminating...")
    :dets.close(@disk_table)
    {:stop, :normal}
  end

  defp update_index(word, value) do
    case :ets.lookup(@index_table, word) do
      [] ->
        true = :ets.insert(@index_table, {word, [value]})
        :ok = :dets.insert(@disk_table, {word, [value]})
      [{^word, index_list}] ->
        true = :ets.insert(@index_table, {word, index_list ++ [value]})
        :ok = :dets.insert(@disk_table, {word, index_list ++ [value]})
    end
  end

  defp replace_index(word, value) do
    true = :ets.insert(@index_table, {word, value})
    :ok = :dets.insert(@disk_table, {word, value})
  end
  def add_record(key, index_list) do
    GenServer.call(__MODULE__, {:add, key, index_list})
  end

  def delete_record(key) do
    GenServer.call(__MODULE__, {:delete, key})
  end

  def lookup(word) when is_binary(word) do
    upcased = String.upcase(word)
    GenServer.call(__MODULE__, {:get, upcased})
  end

  def all_records() do
    GenServer.call(__MODULE__, :all)
  end

  def count_entries() do
    GenServer.call(__MODULE__, :count)
  end

  defp add_key_to_index(key) do
    :dets.insert(@keys_table, {key})
  end

  defp is_key_member?(key) do
    case :dets.lookup(@keys_table, key) do
      [] -> false
      [{^key}] -> true
    end
  end

  defp delete_key_from_index(key) do
    :dets.delete(@keys_table, key)
  end

  def purge() do
    GenServer.call(__MODULE__, :purge)
  end
end
