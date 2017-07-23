
-module(ekka_test).

-compile(export_all).

start_slave(node, Node) ->
    {ok, N} = slave:start(host(), Node, ebin_path()),
    N;
start_slave(ekka, Node) ->
    {ok, Ekka} = slave:start(host(), Node, ebin_path()),
    rpc:call(Ekka, application, ensure_all_started, [ekka]),
    Ekka.

wait_running(Node) ->
    wait_running(Node, 30000).

wait_running(Node, Timeout) when Timeout < 0 ->
    throw({wait_timeout, Node});

wait_running(Node, Timeout) ->
    case rpc:call(Node, ekka, is_running, [Node, ekka]) of
        true  -> ok;
        false -> timer:sleep(100),
                 wait_running(Node, Timeout - 100)
    end.

stop_slave(Node) ->
    slave:stop(Node).

host() -> [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

ebin_path() ->
    EbinDir = local_path(["ebin"]),
    DepsDir = local_path(["deps", "*", "ebin"]),
    "-pa " ++ EbinDir ++ " -pa " ++ DepsDir.

get_base_dir(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

get_base_dir() ->
    get_base_dir(?MODULE).

local_path(Components, Module) ->
    filename:join([get_base_dir(Module) | Components]).

local_path(Components) ->
    local_path(Components, ?MODULE).

