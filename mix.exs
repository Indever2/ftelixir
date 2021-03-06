defmodule Ftelixir.MixProject do
  use Mix.Project

  def project do
    [
      app: :ftelixir,
      version: "0.1.1",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {Ftelixir.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    []
  end
end
