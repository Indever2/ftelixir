defmodule Ftelixir do
  @word_backroll_rate 0.6
  def add_to_index(key, text) do
    prepared = prepare_string(text)

    index_list =
    prepared
    |> Enum.reduce(
      [],
      fn word, acc ->
        acc ++ index_subwords(word)
      end
    )

    Ftelixir.Engine.add_record(key, index_list)
  end

  def delete_key_from_index(key) do
    Ftelixir.Engine.delete_record(key)
  end

  def dump_index() do
    Ftelixir.Engine.all_records()
  end

  def count_entries() do
    Ftelixir.Engine.count_entries()
  end

  def drop_tables() do
    Ftelixir.Engine.purge()
  end

  defp prepare_string(string) when is_binary(string) do
    upcased = String.upcase(string)

    Regex.scan(~r/\w+/u, upcased)
      |> Enum.map(fn [word] -> word end)
  end

  def prepare_string_escape_minus(string) when is_binary(string) do
    upcased = String.upcase(string)

    Regex.scan(~r/\w+|-\w+/u, upcased)
      |> Enum.map(fn [word] -> word end)
  end

  def search(string) do
    search(string, plus_rate: 0.8, minus_rate: 0.65)
  end
  def search(string, params) when is_binary(string) and is_list(params) do
    plus_rate = Keyword.get(params, :plus_rate, 0.8)
    minus_rate = Keyword.get(params, :minus_rate, 0.8)

    prepare_string_escape_minus(string)
      |> Enum.map(fn word ->
        case String.first(word) do
          "-" ->
            {String.slice(word, 1..-1), :minus, minus_rate}
          _ ->
            {word, :plus, plus_rate}
        end
      end)
    |> search_words_lazy
  end

  defp rate_filter(list, rate) when is_list(list) and is_float(rate) do
    Enum.filter(list, fn {_id, match_rate} -> match_rate >= rate end)
      |> Enum.map(fn {id, _} -> id end)
  end

  defp search_words_lazy(stated_words) when is_list(stated_words) do
    {plus_list, minus_list} =
      Enum.reduce(stated_words, {[], []}, fn ({word, state, rate}, {plus_words, minus_words}) ->
        case state do
          :plus ->
            {plus_words ++ [{word, state, rate}], minus_words}
          :minus ->
            {plus_words, minus_words ++ [{word, state, rate}]}
        end
      end)

    swlazy(MapSet.new(), plus_list)
      |> MapSet.new
      |> swlazy(minus_list)
  end

  defp swlazy(index_map, []), do: MapSet.to_list(index_map)
  defp swlazy(index_map, [{word, :minus, rate}|remains]) do
    if MapSet.size(index_map) == 0 do
      []
    else
      to_remove = lookup_word(word)
      |> rate_filter(rate)

      case to_remove do
        [] ->
          swlazy(index_map, remains)
        _ ->
          updated_map = Enum.reduce(to_remove, index_map, fn x, acc -> MapSet.delete(acc, x) end)
          swlazy(updated_map, remains)
      end
    end
  end

  defp swlazy(index_map, [{word, :plus, rate}|remains]) do
    word_search_result = lookup_word(word)
      |> rate_filter(rate)

    case word_search_result do
      [] ->
        []
      _ ->
        if MapSet.size(index_map) == 0 do
          swlazy(MapSet.new(word_search_result), remains)
        else
          swlazy(MapSet.intersection(MapSet.new(word_search_result), index_map), remains)
        end
    end
  end

  defp weight_f(x, x), do: 1
  defp weight_f(x, y) when x >= y do
    diff = x - y
    divide = x / y
    cond do
      x > 8 ->
        case diff do
          1 ->
            0.98
          2 ->
            0.91
          3 ->
            0.86
          4 ->
            0.8
          _ ->
            divide
        end
      x > 5 ->
        case diff do
          1 ->
            0.95
          2 ->
            0.85
          3 ->
            0.76
          _ ->
            divide
        end
      x > 3 ->
        case diff do
          1 ->
            0.9
          2 ->
            0.78
          _ ->
            divide
        end
      true ->
        divide
    end
  end

  def lookup_word(word) do # ремонтный
    upcased = String.upcase(word)
    word_len = String.length(upcased)

    word_range(upcased)
    |> Enum.reduce(
      [],
      fn len, acc ->
        acc ++ [ {String.slice(upcased, 0..(len - 1)), weight_f(word_len, len)} ]
      end
    )
    |> Enum.reduce(
      %{},
      fn {word, coff}, acc ->

          case Ftelixir.Engine.lookup(word) do
            [] ->
              acc
            [{^word, matches}] ->
              min = fn x, y -> if x >= y do
                  y
                else
                  x
                end
              end

              Enum.reduce(
                matches, acc,
                fn {key, match_coff}, sub_acc ->
                  case Map.has_key?(sub_acc, key) do
                    false -> Map.put_new(sub_acc, key, min.(coff, match_coff))
                    true ->
                      if sub_acc[key] < min.(coff, match_coff) do
                        %{sub_acc | key => min.(coff, match_coff)}
                      else
                        sub_acc
                      end
                  end
                end
              )
          end

      end
    )
    |> Map.to_list
  end

  def index_subwords("") do
    raise "Empty strings are not supported"
  end
  def index_subwords(word)
  when is_binary(word)
  do
    word_len = String.length(word)

    word_range(word)
    |> Enum.reduce(
      [],
      fn len, acc ->
        acc ++ [ {String.slice(word, 0..(len - 1)), weight_f(word_len, len)} ]
      end
    )
  end

  def word_range(word) when is_binary(word) do
    word_range(word, @word_backroll_rate)
  end
  def word_range(word, rate) when is_binary(word) and is_float(rate) do
      word_len = String.length(word)

      down_plank =
      if round(word_len * rate) > 3 do
        round(word_len * rate)
      else
        word_len
      end

      word_len..down_plank
      |> Enum.to_list
  end
end
