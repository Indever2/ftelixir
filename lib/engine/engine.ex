defmodule Ftelixir.Engine do
  use GenServer
  require Logger

  defp table_names(index_name) when is_atom(index_name) do
    %{
      index_table: String.to_atom(Atom.to_string(index_name) <> "_index_table"),
      disk_table: String.to_atom(Atom.to_string(index_name) <> "_disk_table"),
      keys_table: String.to_atom(Atom.to_string(index_name) <> "_keys_table")
    }
  end

  def init(%{name: self_name}) do
    %{index_table: index_table_nm, keys_table: keys_table_nm, disk_table: disk_index_table_nm} = table_names(self_name)
    {:ok, disk_table} = :dets.open_file(disk_index_table_nm, [type: :set])
    :ets.new(index_table_nm, [:ordered_set, :protected, :named_table])
    :ets.from_dets(index_table_nm, disk_table)

    {:ok, _} = :dets.open_file(keys_table_nm, [type: :set])

    # Defining the index name and tables
    state = %{
      name: self_name,
      index_table: index_table_nm,
      disk_index_table: disk_index_table_nm,
      keys_table: keys_table_nm
    }

    {:ok, state}
  end

  def start_link(%{name: name} = opts) when is_map(opts) do
    Logger.info("[#{__MODULE__}] Starting the engine: #{inspect name}")
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def handle_call({:add, key, index_list}, _from, state) do
    Enum.map(
      index_list,
      fn {word, coff} ->
        update_index(state, word, {key, coff})
      end
    )
    {:reply, :ok, state}
  end

  def handle_call({:get, word}, _from, state) do
    case :ets.lookup(state.index_table, word) do
      [] -> {:reply, [], state}
      [{^word, _hash, matches}] -> {:reply, [{word, matches}], state}
    end
  end

  def handle_call(:all, _from, state) do
    res = :ets.match_object(state.index_table, {:"$1", :"$2", :"$3"})
    {:reply, res, state}
  end

  def handle_call({:delete, key}, _from, state) do
    res = delete_key_from_index(key, state)
    {:reply, res, state}
  end

  def handle_call(:purge, _from, state) do
    :ok = :dets.delete_all_objects(state.disk_index_table)
    :ok = :dets.delete_all_objects(state.keys_table)
    true = :ets.delete_all_objects(state.index_table)

    {:reply, :purged, state}
  end

  def handle_call(:count, _from, state) do
    res =
      :ets.match_object(state.index_table, {:"$1", :"$2"})
      |> Enum.map(fn {key, entries} -> {key, length(entries)} end)

    {:reply, res, state}
  end

  def terminate(_reason, state) do
    Logger.info("#{state.name} is terminating...")
    :dets.close(state.disk_index_table)
    {:stop, :normal}
  end

  defp update_index(%{index_table: index_table, disk_index_table: disk_table, keys_table: keys_table}, word, {key, _data} = value) do
    hash =
      case :ets.lookup(index_table, word) do
        [] ->
          hash = :crypto.hash(:sha256, word)
          true = :ets.insert(index_table, {word, hash, [value]})
          :ok = :dets.insert(disk_table, {word, hash, [value]})
          hash
        [{^word, hash, index_list}] ->
          true = :ets.insert(index_table, {word, hash, index_list ++ [value]})
          :ok = :dets.insert(disk_table, {word, hash, index_list ++ [value]})
          hash
      end
    :ok = store_key_and_hash(key, hash, keys_table)
  end

  defp replace_index(%{index_table: index_table, disk_index_table: disk_table}, word, value) do
    hash = :crypto.hash(:sha256, word)

    true = :ets.insert(index_table, {word, hash, value})
    :ok = :dets.insert(disk_table, {word, hash, value})
  end
  def add_record(index_name, key, index_list) do
    GenServer.call(index_name, {:add, key, index_list})
  end

  def delete_record(index_name, key) do
    GenServer.call(index_name, {:delete, key})
  end

  def lookup(index_name, word) when is_binary(word) do
    upcased = String.upcase(word)
    GenServer.call(index_name, {:get, upcased})
  end

  def lookup_hash(index_name, hash) do
    GenServer.call(index_name, {:get, :hash, hash})
  end

  def all_records(index_name) do
    GenServer.call(index_name, :all)
  end

  def count_entries(index_name) do
    GenServer.call(index_name, :count)
  end

  defp store_key_and_hash(key, hash, keys_table) do
    case :dets.lookup(keys_table, key) do
      [] ->
        :dets.insert(keys_table, {key, [hash]})
      [{key, hashes}] ->
        :dets.insert(keys_table, {key, hashes ++ [hash]})
    end
  end

  defp delete_key_from_index(key, %{keys_table: keys_table, index_table: index_table, disk_index_table: disk_table} = tables) do
    res = case :dets.lookup(keys_table, key) do
      [] -> {:ok, :nothing_to_do}
      [{^key, hashes}] ->
        for hash <- hashes do
          case :ets.match_object(index_table, {:"$1", hash, :"$3"}) do # TODO: func get_from_index_by_hash()
            [] -> :ok # maybe shold be replaced
            [{word, ^hash, references}] ->
              case Enum.filter(references, fn {entry_key, _data} -> entry_key != key end) do
                [] ->
                  true = :ets.delete(index_table, word)
                  :ok = :dets.delete(disk_table, word)
                updated ->
                  replace_index(tables, word, updated)
              end
          end
        end
        {:ok, :deleted}
    end
    :ok = :dets.delete(keys_table, key)
    res
  end

  def purge(index_name) do
    GenServer.call(index_name, :purge)
  end
end
