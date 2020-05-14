defmodule Ftelixir do
  import Ftelixir.Default

  # Callbacks to build your own great full-text search!
  @callback on_add_filter(str :: binary()) :: list()
  @callback on_add_function(words_list :: list()) :: list()
  @callback on_search_filter(str :: binary()) :: list()
  @callback search_function(filtered_input :: list()) :: list()

  @optional_callbacks on_add_filter: 1, on_add_function: 1, on_search_filter: 1, search_function: 1

  # ============================================================
  # Programming interface section
  # ============================================================
  def add_to_index(key, text), do: add_to_index(Ftelixir, :default, key, text)
  def add_to_index(index_name, key, text) when is_atom(index_name) do
    add_to_index(Ftelixir, index_name, key, text)
  end
  def add_to_index(module, index_name, key, text) do
    index_not_exists_raiser(index_name)

    filtered = try do
      apply(module, :on_add_filter, [text])
    rescue
      _ in UndefinedFunctionError -> input_filter_default(text)
    end

    index_list = try do
      apply(module, :on_add_function, [filtered])
     rescue
      _ in UndefinedFunctionError -> add_function_default(filtered)
    end

    Ftelixir.Engine.add_record(index_name, key, index_list)
  end

  def search(text) do
    search(Ftelixir, :default, text)
  end
  def search(index_name, text) when is_atom(index_name) do
    search(Ftelixir, index_name, text)
  end
  def search(module, index_name, text) when is_atom(index_name) do
    index_not_exists_raiser(index_name)

    filtered = try do
      apply(module, :on_search_filter, [text])
    rescue
      _ in UndefinedFunctionError -> input_filter_default(text)
    end

    result = try do
      apply(module, :search_function, [filtered])
     rescue
      _ in UndefinedFunctionError -> search_function_default(index_name, filtered)
    end

    result
  end

  def create_index(index_name) when is_atom(index_name) do
    Ftelixir.IndexManager.create_index(index_name)
  end

  def delete_key_from_index(key), do: delete_key_from_index(:default, key)
  def delete_key_from_index(index, key) do
    index_not_exists_raiser index
    Ftelixir.Engine.delete_record(index, key)
  end

  def dump_index(), do: dump_index(:default)
  def dump_index(index_name) do
    index_not_exists_raiser index_name
    Ftelixir.Engine.all_records(index_name)
  end

  def count_entries(), do: count_entries(:default)
  def count_entries(index_name) do
    index_not_exists_raiser index_name
    Ftelixir.Engine.count_entries(index_name)
  end

  @doc "Deletes all records in index."
  def drop_tables(), do: drop_tables(:default)
  def drop_tables(index_name) do
    index_not_exists_raiser index_name
    Ftelixir.Engine.purge(index_name)
  end

  defp index_not_exists_raiser(index_name) do
    case Ftelixir.IndexManager.ensure_index_exists(index_name) do
      :ok ->
        :ok
      _ ->
        raise "Index #{inspect index_name} does not exists!"
    end
  end
end
