defmodule Ftelixir.IndexManager do
  use GenServer
  require Logger

  def init(_) do
    {:ok, table} = :dets.open_file(:indexes, [type: :set])
    indexes = :dets.match_object(table, {:"$1"})

    children = case indexes do
      [] ->
        :dets.insert(table, {:default})
        [%{id: :default, start: {Ftelixir.Engine, :start_link, [%{name: :default}]}}]
      index_list ->
        for {index_name} <- index_list do
          %{id: :index_name, start: {Ftelixir.Engine, :start_link, [%{name: index_name}]}}
        end
    end

    {:ok, pid} = Supervisor.start_link(children, strategy: :one_for_one, name: Ftelixir.IndexSupervisor)
    {:ok, %{indexes: Enum.map(indexes, fn {x} -> x end), supervisor: pid, index_table: table}}
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def handle_call(:indexes, _from, %{indexes: indexes} = state) do
    {:reply, {:ok, indexes}, state}
  end

  def handle_call({:create_index, index_name}, _from, %{indexes: indexes, supervisor: supervisor, index_table: table} = state) do
    if index_name not in indexes do
      {:ok, pid} = Supervisor.start_child(supervisor, %{id: index_name, start: {Ftelixir.Engine, :start_link, [%{name: index_name}]}})
      :dets.insert(table, {index_name})
      {:reply, {:ok, pid}, %{state | indexes: indexes ++ [index_name]}}
    else
      {:reply, {:err, :index_already_exists}, state}
    end
  end

  def create_index(index_name) when is_atom(index_name) do
    GenServer.call(__MODULE__, {:create_index, index_name})
  end

  def ensure_index_exists(index_name) do
    {:ok, indexes} = GenServer.call(__MODULE__, :indexes)

    if index_name not in indexes do
      {:error, :not_exists}
    else
      :ok
    end
  end
end
