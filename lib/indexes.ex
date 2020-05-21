defmodule Ftelixir.IndexManager do
  use GenServer
  require Logger

  def init(_) do
    {:ok, table} = :dets.open_file(:indexes, [type: :set])
    indexes = :dets.match_object(table, {:"$1", :"$2"})

    children = case indexes do
      [] ->
        Logger.info("[#{__MODULE__}] Creating the index: :default")
        :dets.insert(table, {:default, %{}})
        [%{id: :default, start: {Ftelixir.Engine, :start_link, [%{name: :default}]}}]
      index_list ->
        for {index_name, _index_properties} <- index_list do
          %{id: index_name, start: {Ftelixir.Engine, :start_link, [%{name: index_name}]}}
        end
    end

    indexes = :dets.match_object(table, {:"$1", :"$2"})

    {:ok, pid} = Supervisor.start_link(children, strategy: :one_for_one, name: Ftelixir.IndexSupervisor)
    {:ok, %{indexes: indexes, supervisor: pid, index_table: table}}
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def handle_call(:indexes, _from, %{indexes: indexes} = state) do
    {:reply, {:ok, index_names(indexes)}, state}
  end

  def handle_call({:get, index_name}, _from, %{indexes: indexes} = state) do
    case Keyword.get(indexes, index_name) do
      nil ->
        {:reply, {:error, "Index does not exist!"}, state}
      res ->
        {:reply, Map.put(res, :name, index_name), state}
    end
  end

  def handle_call({:create_index, index_name, %{} = properties}, _from, %{indexes: indexes, supervisor: supervisor, index_table: table} = state) do
    if index_name not in index_names(indexes) do
      {:ok, pid} = Supervisor.start_child(supervisor, %{id: index_name, start: {Ftelixir.Engine, :start_link, [%{name: index_name}]}})
      :dets.insert(table, {index_name, properties})
      {:reply, {:ok, pid}, %{state | indexes: indexes ++ [{index_name, properties}]}}
    else
      {:reply, {:error, :index_already_exists}, state}
    end
  end

  def create_index(index_name, %{} = properties) when is_atom(index_name) do
    GenServer.call(__MODULE__, {:create_index, index_name, properties})
  end

  def get_index(index_name) do
    GenServer.call(__MODULE__, {:get, index_name})
  end

  def index_names(indexes) when is_list(indexes) do
    Keyword.keys(indexes)
  end
end
