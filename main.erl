-module(main).
-export([start/0, actorEntry/4, stdinReadLoop/0]).
%-compile(export_all).

% NOTE: Any function called with spawn/ MUST be exported
% You can do this by listing them explicitly in -export([foo/1])
% of with -compile(export_all), which'll give you a compilation warning
% You also need to export a function to create an explicit reference to
% it for higher-order programming.

getRandomNode() ->
  World = net_adm:world(),
  Rand = rand:uniform(length(World)),
  lists:nth(Rand, World).

getPid(N, PidMap) ->
  maps:get(N, PidMap).

normed(Curr, Node, Nodes) -> (Node - Curr + Nodes) rem Nodes.

invnormed(Curr, Node, Nodes) -> (Node + Curr) rem Nodes.

nextHop(Curr, Dst, Nodes) ->
  DestNorm = normed(Curr, Dst, Nodes), % Destination if we were to treat Curr
                                       % as 0 (normed destination)
  NextHopNorm = trunc(math:pow(2, trunc(math:log2(DestNorm)))), % Highest power of two that is
                                            % Less than the normed destination
  invnormed(Curr, NextHopNorm, Nodes). % Convert back to global index


appendMapList(Map, Key, Val) ->
  A = fun (Original) ->
          lists:append([Val] , Original)
      end,
  maps:update_with(Key, A, [Val], Map).




strPIDs(PIDMap, Nodes, Cur) ->
  if
    Nodes == Cur -> "";
    true ->
      PID = maps:get(Cur, PIDMap),
      lists:flatten(io_lib:fwrite("{ index: ~b, pid: ~w } ~s", [Cur, PID, strPIDs(PIDMap, Nodes, Cur+1)]))
  end.

actorEntry(Me, LocalMap, Awaiting, Nodes) ->
  receive
    PIDMap ->
      processDbg(Me, strPIDs(PIDMap, Nodes, 0), []),
      actorEl(Me, PIDMap, LocalMap, Awaiting, Nodes)
  end.

queryStr(Args, Nodes) ->
  [_, _, Key] = Args,
  lists:flatten(io_lib:fwrite("Query{ qid: ~b, origin: ~b, key: ~s, destination: ~b }", 
                              lists:append(Args, [util:hash(Key, Nodes)]))).

insertStr(Args, Nodes) ->
  [_, Key, _] = Args,
  lists:flatten(io_lib:fwrite("Insert{ origin: ~b, key: ~s, value: ~s, destination: ~b }", 
                              lists:append(Args, [util:hash(Key, Nodes)]))).

processDbg(Me, FStr, Output) ->
  io:fwrite("Process[~b]: ~s~n", [Me, lists:flatten(io_lib:fwrite(FStr,
                                                                  Output))]).

actorEl(Me, PIDMap, LocalMap, Awaiting, Nodes) ->
  processDbg(Me, "ActorEl started", []),
  receive
    {insert, [Origin, Key, Value]} ->
      Dst = util:hash(Key, Nodes),
      if
        Dst == Me ->
          processDbg(Me, "Destination of ~s Reached", [insertStr([Origin, Key, Value], Nodes)]),
          case maps:find(Key, Awaiting) of
            {ok, QList} ->
              SFunc = fun (Args) ->
                          processDbg(Me, "Re-Queuing ~s", [queryStr(Args, Nodes)]),
                          self() ! {query, Args}
                      end,
              lists:foreach(SFunc, QList),
              actorEl(Me, PIDMap, maps:put(Key, Value, LocalMap), maps:remove(Key,
                                                                      Awaiting),
                      Nodes);
            error ->
              actorEl(Me, PIDMap, maps:put(Key, Value, LocalMap), Awaiting, Nodes)
          end;
        true ->
          NextNode = nextHop(Me, Dst, Nodes),
          processDbg(Me, "In Transit: ~s, next hop: Process[~b]", [insertStr([Origin, Key, Value], Nodes), NextNode]),
          getPid(NextNode, PIDMap) ! {insert, [Origin, Key, Value]},
          actorEl(Me, PIDMap, LocalMap, Awaiting, Nodes)
      end;
    {query, [QID, Origin, Key]} ->
      Dst = util:hash(Key, Nodes),
      if 
        Dst == Me ->
          case maps:find(Key, LocalMap) of
            {ok, Value} ->
              getPid(-1, PIDMap) ! {result, [QID, Origin, Key, Value]},
              processDbg(Me, "Destination of ~s Reached and fulfilled", [queryStr([QID, Origin, Key], Nodes)]),
              actorEl(Me, PIDMap, LocalMap, Awaiting, Nodes);
            error ->
              processDbg(Me, "Destination of ~s Reached and waiting", [queryStr([QID, Origin, Key], Nodes)]),
              actorEl(Me, PIDMap, LocalMap, appendMapList(Awaiting, Key, 
                                                  [QID, Origin, Key]), Nodes)
          end;
        true ->
          NextNode = nextHop(Me, Dst, Nodes),
          processDbg(Me, "In Transit: ~s, next hop: Process[~b]", [queryStr([QID, Origin, Key], Nodes), NextNode]),
          getPid(NextNode, PIDMap) ! {query, [QID, Origin, Key]},
          actorEl(Me, PIDMap, LocalMap, Awaiting, Nodes)
      end
  end.

%register(getPid(N-1), spawn(getRandomNode(), main, actorEl, [N-1, #{}, #{}, Nodes])),
startActors(N, Nodes) ->
  if
    N > 0 ->
      PID = spawn(getRandomNode(), main, actorEntry, [N-1, #{}, #{}, Nodes]),
      maps:merge(#{N-1 => PID}, startActors(N-1, Nodes));
    true -> #{-1 => self()}
  end.

handleInsert(Origin, Key, Value, PIDMap) -> 
  getPid(Origin, PIDMap) ! {insert, [Origin, Key, Value]}.
handleQuery(QID, Origin, Key, PIDMap) ->
  getPid(Origin, PIDMap) ! {query, [QID, Origin, Key]}.
handleResult(QID, Origin, Key, Value, Nodes) ->
  io:format("Request ~b sent to agent ~b: Value for key \"~s\" stored in node ~b: \"~s\"~n", [QID, Origin, Key, util:hash(Key, Nodes), Value]).

processRequests(Nodes, PIDMap) ->
  receive
    {insert, [Origin, Key, Value]} ->
      handleInsert(Origin, Key, Value, PIDMap);
    {query, [QID, Origin, Key]} ->
      handleQuery(QID, Origin, Key, PIDMap);
    {result, [QID, Origin, Key, Value]} ->
      handleResult(QID, Origin, Key, Value, Nodes)
  end,
  processRequests(Nodes, PIDMap).

stdinReadLoop() ->
  {ok, [RequestString]} = io:fread("", "~s"),
  Request = list_to_atom(RequestString),
  case Request of
    insert ->
      {ok, [Origin, Key, Value]} = io:fread("", "~d ~s ~s"),
      clientPID ! {insert, [Origin, Key, Value]};
    query ->
      {ok, [QID, Origin, Key]} = io:fread("", "~d ~d ~s"),
      clientPID ! {query, [QID, Origin, Key]};
    _ -> io:format("ERROR: Illegal request ~p~n", [Request])
  end,
  stdinReadLoop().

start() ->
	{ok, [N]} = io:fread("", "~d"),
	Nodes = round(math:pow(2, N)),
  PIDMap = startActors(Nodes, Nodes),
  F = fun (K, V) ->
          if
            K >= 0 ->
              V ! PIDMap,
              0;
            true -> 0
          end
      end,
  maps:foreach(F, PIDMap),
  register(clientPID, self()),
  spawn(main, stdinReadLoop, []),
  processRequests(Nodes, PIDMap).
  
%io:format("With ~b nodes, key \"test\" belongs in node ~b~n", [Nodes, util:hash("test", Nodes)]),
