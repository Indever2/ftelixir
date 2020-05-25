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

  def add_to_index(key, text), do: add_to_index(key, text, nil, nil, nil)
  def add_to_index(key, text, property), do: add_to_index(key, text, property, nil, nil)
  def add_to_index(key, text, property, index), do: add_to_index(key, text, property, index, nil)
  def add_to_index(key, text, property, index_name, module) do
    index_name = default_if_nil(index_name, :default)
    module = default_if_nil(module, Ftelixir)

    index = get_index_info! (index_name)

    filtered = try do
      apply(module, :on_add_filter, [text])
    rescue
      _ in UndefinedFunctionError -> input_filter_default(text)
    end

    index_list = try do
      apply(module, :on_add_function, [filtered, index])
     rescue
      _ in UndefinedFunctionError -> add_function_default(filtered, index)
    end

    Ftelixir.Engine.add_record(index_name, key, property, index_list)
  end

  def search(text), do: search(text, nil, nil, nil, nil)
  def search(text, options), do: search(text, options, nil, nil, nil)
  def search(text, options, properties) when is_list(properties), do: search(text, options, properties, nil, nil)
  def search(text, options, properties, index_name), do: search(text, options, properties, index_name, nil)
  def search(text, options, properties, index_name, module) do
    properties = default_if_nil(properties, Keyword.new())
    index_name = default_if_nil(index_name, :default)
    module = default_if_nil(module, Ftelixir)

    get_index_info! (index_name)

    filtered = try do
      apply(module, :on_search_filter, [text])
    rescue
      _ in UndefinedFunctionError -> input_filter_default(text)
    end

    result = try do
      apply(module, :search_function, [filtered])
     rescue
      _ in UndefinedFunctionError -> search_function_default(index_name, filtered, options, properties)
    end

    result
  end

  def create_index(index_name), do: create_index(index_name, %{})
  def create_index(index_name, %{} = properties) when is_atom(index_name) do
    Ftelixir.IndexManager.create_index(index_name, properties)
  end

  def delete_key_from_index(key), do: delete_key_from_index(:default, key)
  def delete_key_from_index(index, key) do
    get_index_info! index
    Ftelixir.Engine.delete_record(index, key)
  end

  def dump_index(), do: dump_index(:default)
  def dump_index(index_name) do
    get_index_info! index_name
    Ftelixir.Engine.all_records(index_name)
  end

  def count_entries(), do: count_entries(:default)
  def count_entries(index_name) do
    get_index_info! index_name
    Ftelixir.Engine.count_entries(index_name)
  end

  @doc "Deletes all records in index."
  def drop_tables(), do: drop_tables(:default)
  def drop_tables(index_name) do
    get_index_info! index_name
    Ftelixir.Engine.purge(index_name)
  end

  def get_index_info!(index_name) do
    case Ftelixir.IndexManager.get_index(index_name) do
      {:error, _} ->
        raise "Index #{inspect index_name} does not exists!"
      %{name: ^index_name} = index ->
        index
    end
  end

  def default_if_nil(value, default) do
    if is_nil(value) do
      default
    else
      value
    end
  end
end
