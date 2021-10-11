-module(util).
-export([hash/2, getRandomNode/0]).

getRandomNode() ->
	World = net_adm:world(),
	Rand = rand:uniform(length(World)),
	lists:nth(Rand, World).



% Hash function returns (fold + [ord(c) for c in string]) % nodes
% In other words, function takes (key, number_of_nodes) and returns
% the node to store the key in.
% It is a very bad hash function, but sufficient for our
% purposes of very roughly evenly distributing keys among nodes
hash(D, [], Nodes) -> D rem Nodes;
hash(D, [C|Cs], Nodes) -> hash(D+C, Cs, Nodes).
hash(S, Nodes) -> hash(0, S, Nodes).
