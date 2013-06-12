-module(ernie).
  -export([start/0,  start_link/1, start/1, process/1, enqueue_request/1, kick/0, fin/0]).
-behaviour(gen_server).
  -export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {lsock = undefined,      % the listen socket
                listen = true,          % whether to listen for new connections
                hq = queue:new(),       % high priority queue
                lq = queue:new(),       % low priority queue
                count = 0,              % total request count
                zcount = 0,             % total completed request count
                modules = undefined}).  % modules allowed for RPC

-record(request, {sock = undefined,     % connection socket
                  infos = [],           % list of info binaries
                  action = undefined,   % action binary
                  priority = high}).    % priority [ high | low ]


%%====================================================================
%% API
%%====================================================================

start() ->
  application:start(ernie).


start_link(Args) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

start(Args) ->
  gen_server:start({local, ?MODULE}, ?MODULE, Args, []).

process(Sock) ->
  gen_server:cast(?MODULE, {process, Sock}).

enqueue_request(Request) ->
  gen_server:call(?MODULE, {enqueue_request, Request}).

kick() ->
  gen_server:cast(?MODULE, kick).

fin() ->
  gen_server:cast(?MODULE, fin).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init(_Args) ->
  process_flag(trap_exit, true),

  {ok, Port} = application:get_env(ernie, port),
  {ok, AllowedModules} = application:get_env(ernie, allowed_modules),

  {ok, LSock} = try_listen(Port, 500),
  spawn(fun() -> loop(LSock) end),
  Modules = sets:from_list(AllowedModules),
  {ok, #state{lsock = LSock, modules = Modules}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({enqueue_request, Request}, _From, State) ->
  case Request#request.priority of
    high ->
      Hq2 = queue:in(Request, State#state.hq),
      Lq2 = State#state.lq;
    low ->
      Hq2 = State#state.hq,
      Lq2 = queue:in(Request, State#state.lq)
  end,
  {reply, ok, State#state{hq = Hq2, lq = Lq2}};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({process, Sock}, State) ->
  Request = #request{sock = Sock},
  spawn(fun() -> receive_term(Request, State) end),
  {noreply, State};
handle_cast(kick, State) ->
  case queue:out(State#state.hq) of
    {{value, Request}, Hq2} ->
      State2 = process_request(Request, hq, Hq2, State),
      {noreply, State2};
    {empty, _Hq} ->
      case queue:out(State#state.lq) of
        {{value, Request}, Lq2} ->
          State2 = process_request(Request, lq, Lq2, State),
          {noreply, State2};
        {empty, _Lq} ->
          {noreply, State}
      end
  end;
handle_cast(fin, State) ->
  Listen = State#state.listen,
  Count = State#state.count,
  ZCount = State#state.zcount + 1,
  case Listen =:= false andalso ZCount =:= Count of
    true -> halt();
    false -> {noreply, State#state{zcount = ZCount}}
  end;
handle_cast(_Msg, State) -> {noreply, State}.

handle_info(Msg, State) ->
  error_logger:error_msg("Unexpected message: ~p~n", [Msg]),
  {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVersion, State, _Extra) -> {ok, State}.

%%====================================================================
%% Internal
%%====================================================================

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Listen and loop

try_listen(Port, 0) ->
  error_logger:error_msg("Could not listen on port ~p~n", [Port]),
  {error, "Could not listen on port"};
try_listen(Port, Times) ->
  Res = gen_tcp:listen(Port, [binary, {packet, 4}, {active, false}, {reuseaddr, true}, {backlog, 128}]),
  case Res of
    {ok, LSock} ->
      % error_logger:info_msg("Listening on port ~p", [Port]),
      % gen_tcp:controlling_process(LSock, ernie),
      {ok, LSock};
    {error, Reason} ->
      error_logger:info_msg("Could not listen on port ~p: ~p~n", [Port, Reason]),
      timer:sleep(5000),
      try_listen(Port, Times - 1)
  end.

loop(LSock) ->
  case gen_tcp:accept(LSock) of
    {error, closed} ->
      timer:sleep(infinity);
    {error, _} ->
      loop(LSock);
    {ok, Sock} ->
      ?MODULE:process(Sock),
      loop(LSock)
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Receive and process

receive_term(Request, State) ->
  Sock = Request#request.sock,
  case gen_tcp:recv(Sock, 0) of
    {ok, BinaryTerm} ->
      Term = binary_to_term(BinaryTerm),
      case Term of
        {info, Command, Args} ->
          Infos = Request#request.infos,
          Infos2 = [BinaryTerm | Infos],
          Request2 = Request#request{infos = Infos2},
          Request3 = process_info(Request2, Command, Args),
          receive_term(Request3, State);
        _Any ->
          Request2 = Request#request{action = BinaryTerm},
          close_if_cast(Term, Request2),
          ?MODULE:enqueue_request(Request2),
          ?MODULE:kick()
      end;
    {error, closed} ->
      ok = gen_tcp:close(Sock)
  end.

process_info(Request, priority, [Priority]) ->
  Request#request{priority = Priority};
process_info(Request, _Command, _Args) ->
  Request.

process_request(Request, Priority, Q2, State) ->
  ActionTerm = bert:decode(Request#request.action),
  {_Type, Mod, Fun, Args} = ActionTerm,
  case sets:is_element(Mod, State#state.modules) of
    true ->
      code:ensure_loaded(Mod),
      case erlang:function_exported(Mod, Fun, length(Args)) of
        true -> process_native_request(ActionTerm, Request, Priority, Q2, State);
        false -> no_function(ActionTerm, Request, Priority, Q2, State)
      end;
    false -> no_function(ActionTerm, Request, Priority, Q2, State)
  end.

process_native_request(ActionTerm, Request, Priority, Q2, State) ->
  Count = State#state.count,
  State2 = State#state{count = Count + 1},

  spawn(fun() -> 
    {_Type, Mod, Fun, Args} = ActionTerm,
    Sock = Request#request.sock,
    try apply(Mod, Fun, Args) of
      Result ->
        Data = bert:encode({reply, Result}),
        gen_tcp:send(Sock, Data)
    catch
      error:Error ->
        BError = list_to_binary(io_lib:format("~p", [Error])),
        Trace = erlang:get_stacktrace(),
        BTrace = lists:map(fun(X) -> list_to_binary(io_lib:format("~p", [X])) end, Trace),
        Data = term_to_binary({error, [user, 0, <<"RuntimeError">>, BError, BTrace]}),
        gen_tcp:send(Sock, Data)
    end,
    ok = gen_tcp:close(Sock),
    ?MODULE:fin()
  end),

  finish(Priority, Q2, State2).

no_function(ActionTerm, Request, Priority, Q2, State) ->
  {_Type, Mod, Fun, Args} = ActionTerm,
  Sock = Request#request.sock,
  Class = <<"ServerError">>,
  Message = list_to_binary(io_lib:format("No such function '~p:~p/~p'", [Mod, Fun, length(Args)])),
  gen_tcp:send(Sock, term_to_binary({error, [server, 0, Class, Message, []]})),
  ok = gen_tcp:close(Sock),
  finish(Priority, Q2, State).

close_if_cast(ActionTerm, Request) ->
  case ActionTerm of
    {cast, _Mod, _Fun, _Args} ->
      Sock = Request#request.sock,
      gen_tcp:send(Sock, term_to_binary({noreply})),
      ok = gen_tcp:close(Sock);
    _Any ->
      ok
  end.

finish(Priority, Q2, State) ->
  case Priority of
    hq -> State#state{hq = Q2};
    lq -> State#state{lq = Q2}
  end.
