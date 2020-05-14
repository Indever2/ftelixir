defmodule Ftelixir.Default do
  @moduledoc """
  Contains default preprocess and search functions.
  """

  @doc """
  Default filter function
  ## Examples
    iex(3)> Ftelixir.Default.input_filter_default("It's pretty cool, isn't it? :)")
    ["IT", "S", "PRETTY", "COOL", "ISN", "T", "IT"]
  """
  @spec input_filter_default(binary) :: [binary]
  def input_filter_default(string) when is_binary(string) do
    upcased = String.upcase(string)

    Regex.scan(~r/\w+/u, upcased)
      |> Enum.map(fn [word] -> word end)
  end

  @doc """
  Default add function.
  That default function converts every word in the document to list of tuples, that contains
  sliced words (eg Transform -> [transform, transfor, transfo, transf]), len for every sliced word
  and position in document.
  ## Examples
    Ftelixir.add_to_index(1, "I love that transformation so much!")
    [
      {"I", {1, 0}},
      {"LOVE", {4, 1}},
      {"LOV", {3, 1}},
      {"THAT", {4, 2}},
      {"THA", {3, 2}},
      {"TRANSFORMATION", {14, 3}},
      {"TRANSFORMATIO", {13, 3}},
      {"TRANSFORMATI", {12, 3}},
      {"TRANSFORMAT", {11, 3}},
      {"TRANSFORMA", {10, 3}},
      {"TRANSFORM", {9, 3}},
      {"SO", {2, 4}},
      {"MUCH", {4, 5}},
      {"MUC", {3, 5}}
    ]
  """
  def add_function_default(words) when is_list(words) do
    {words, _words_count} = Enum.reduce(words, {[], 0},
    fn word, {acc_words, word_position} ->
    {acc_words ++ [{word, word_position}], word_position + 1}
    end)

    Enum.reduce(words, [], fn {word, pos}, acc ->
      acc ++ Enum.map(slice(word), &({&1, {String.length(&1), pos}})) # list of tuples {"word", {WORD_MATCH_SCORE, WORD_POSITION_IN_DOCUMENT}}
    end)
  end

  def slice(word) do
    word_len = String.length(word)
    do_slice = fn (word, range) -> Enum.map(range, fn i -> String.slice(word, Range.new(0, i)) end) end
    cond do
      word_len > 6 ->
        do_slice.(word, -1..-(round(word_len * 0.4)))
      word_len >= 4 ->
        do_slice.(word, -1..-2)
      word_len > 2 ->
        do_slice.(word, -1..-2)
      true ->
        [word]
    end
  end

  def search_function_default(index_name, words_to_lookup) when is_atom(index_name) do
    # Block 1: getting the results!
    # =============================
    scores_map =
    Enum.reduce(words_to_lookup, [],
    fn word, main_acc ->
      main_acc ++
      Enum.reduce(slice(word), [], fn subword, acc -> acc ++ Ftelixir.Engine.lookup(index_name, subword) end)
    end)
    # Block 2: calculating scores!
    # =============================
    |> Enum.reduce(%{},
    fn {word, entries}, score_map ->
      Enum.reduce(entries, score_map,
      fn {doc_id, {score, doc_position}}, score_submap ->
        map_key = {doc_id, doc_position}
        if Map.has_key?(score_submap, map_key) do
          {_map_word, map_score} = Map.get(score_submap, map_key)
          if score > map_score do
            Map.put(score_submap, map_key, {word, score})
          else
            score_submap
          end
        else
          Map.put(score_submap, map_key, {word, score})
        end
      end)
    end)

    # Block 3: representation
    # =============================
    results = Map.keys(scores_map)
    |> Enum.reduce(%{},
    fn {doc_id, doc_position} = key, acc ->
      {word, scores} = Map.get(scores_map, key)
      if Map.has_key?(acc, doc_id) do
        matches = Map.get(acc, doc_id) ++ [{word, doc_position, scores}]
        Map.put(acc, doc_id, matches)
      else
        Map.put(acc, doc_id, [{word, doc_position, scores}])
      end
    end)
    |> Enum.reduce([], fn {doc_id, matches}, acc ->
      total_score = Enum.reduce(matches, 0, fn {_word, _pos, score}, counter -> counter + score end)
      match = %{
        id: doc_id,
        score: total_score,
        matches: Enum.map(matches, fn {word, pos, score} -> %{match_word: word, position: pos, match_score: score} end)
      }
      acc ++ [match]
    end)

    total =
      %{
        results: length(results),
        max_score: if results != [] do
          Enum.map(results, fn x -> x.score end)
          |> Enum.max
        else
          nil
        end,
        matches: if results != [] do
          Enum.sort(results, &(&1.score >= &2.score))
        else
          []
        end
      }
    total
  end
end
