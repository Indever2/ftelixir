defmodule Ftelixir.Application do
  use Application

  def start(_type, _args) do
    children = [
      Ftelixir.Engine
    ]

    opts = [strategy: :one_for_one, name: Ftelixir.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def stop() do
    Supervisor.terminate_child(Ftelixir.Supervisor, Ftelixir.Engine)
  end
end
