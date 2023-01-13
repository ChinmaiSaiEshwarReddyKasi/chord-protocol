-module(project3).
-import(io, [fwrite/1, fwrite/2, format/2]).
-export([main/2, chordProtocol/1, createProcesses/2, computeHashValue/2, integerToAtom/1, search/4, generateFingerTable/5, createChordCircle/2]).


search(NodeList, _, Low, High) when (High < Low) or ((High =:= 0) and (Low =:= 0))->
    SuccessorIndex = if 
        ((High+1) rem length(NodeList)) =:= 0 -> High+1;
        true -> (High+1) rem length(NodeList)
        end,
    lists:nth(SuccessorIndex, NodeList);
search(NodeList, Target, Low, High) ->
    % format("Low = ~p, High = ~p~n", [Low, High]),
    Mid = round((Low + High) / 2),
    % format("Mid = ~p~n", [Mid]),
    Value = lists:nth(Mid, NodeList),
    if 
        (Target < Value) -> search(NodeList, Target, Low, Mid-1);
        (Target > Value) -> search(NodeList, Target, Mid+1, High);
        (Target =:= Value) -> Value
    end.

generateFingerTable(_, _, _, [], FingerTable) ->
    FingerTable;
generateFingerTable(NodeList, OwnIdentity, M, Indexes, FingerTable) ->
    [I | Tail] = Indexes,
    % format("~p ~p ~p~n", [OwnIdentity, round(OwnIdentity + math:pow(2, I-1)), round(math:pow(2, M))]),
    Id = round(OwnIdentity + math:pow(2, I-1)) rem round(math:pow(2, M)),
    % format("Id = ~p~n", [Id]),
    Result = search(NodeList, Id, 0, length(NodeList)),
    NewFingerTable = lists:append(FingerTable, [Result]),
    generateFingerTable(NodeList, OwnIdentity, M, Tail, NewFingerTable).


chordProtocol(State) ->
    receive
        {initializeNode, NodeIndex, NodeList, M} ->
            % format("Initializing Index =~p~n",[NodeIndex]),
            Len = length(NodeList),
            SuccessorIndex = 
                if 
                    (NodeIndex+1) rem Len =:= 0 -> Len;
                    true -> (NodeIndex+1) rem Len
                end,
            PredecessorIndex = 
                if 
                    (NodeIndex) =:= 1 -> Len;
                    true -> NodeIndex - 1
                end,
            SuccessorId = lists:nth(SuccessorIndex, NodeList),
            PredecessorId = lists:nth(PredecessorIndex, NodeList),
            OwnIdentity = lists:nth(NodeIndex, NodeList),
            FingerTable = generateFingerTable(NodeList, OwnIdentity, M, lists:seq(1,M), []),
            NewState = dict:from_list([
                {'SuccessorIndex', SuccessorIndex}, {'PredecessorIndex', PredecessorIndex}, 
                {'SuccessorId', SuccessorId}, {'PredecessorId', PredecessorId}, {'OwnIdentity', OwnIdentity},
                {'FingerTable', FingerTable}
            ]),
            format("State = ~p~n", [NewState]),
            chordProtocol(NewState);

        {join, NodeId} ->
            SuccessorIndex = nil,
            PredecessorIndex = nil,
            SuccessorId = nil,
            PredecessorId =nil,
            OwnIdentity = NodeId,
            FingerTable = [],
            NewState = dict:from_list([
                {'SuccessorIndex', SuccessorIndex}, {'PredecessorIndex', PredecessorIndex}, 
                {'SuccessorId', SuccessorId}, {'PredecessorId', PredecessorId}, {'OwnIdentity', OwnIdentity},
                {'FingerTable', FingerTable}
            ]),
            format("Join State = ~p~n", [NewState]),
            chordProtocol(NewState);

        {findSuccessor, NodeId, NodeList} ->
            Nodes = lists:sort(NodeList ++ [NodeId]),
            SuccessorIndex = findNode(1, NodeId, Nodes)+1,
            SuccessorId = lists:nth(SuccessorIndex, Nodes),
            integerToAtom(NodeId) ! {setSuccessor, SuccessorId, SuccessorIndex},
            chordProtocol(State);

        {setSuccessor, SuccessorId, SuccessorIndex} ->
            Temp = dict:store('SuccessorIndex', SuccessorIndex, State),
            NewState = dict:store('SuccessorId', SuccessorId, Temp),
            format("Join State After Successor = ~p~n", [NewState]),
            chordProtocol(NewState);

        {setPredecessor, PredecessorId, PredecessorIndex} ->
            Temp = dict:store('PredecessorIndex', PredecessorIndex, State),
            NewState = dict:store('PredecessorId', PredecessorId, Temp),
            format("State After Predecessor = ~p~n", [NewState]),
            chordProtocol(NewState);

        {stabilize, NodeList} ->
            SuccessorId = dict:fetch('SuccessorId', State),
            SuccessorIndex = dict:fetch('SuccessorIndex', State),
            OwnIdentity = dict:fetch('OwnIdentity', State),
            PredecessorData = getPredecessorData(SuccessorId, NodeList),
            PredecessorIndex = element(1, PredecessorData),
            PredecessorId = element(2, PredecessorData),

            integerToAtom(PredecessorId) ! {setSuccessor, OwnIdentity, SuccessorIndex-1},
            integerToAtom(SuccessorId) ! {setPredecessor, OwnIdentity, PredecessorIndex+1},

            Temp = dict:store('PredecessorIndex', PredecessorIndex, State),
            NewState = dict:store('PredecessorId', PredecessorId, Temp),
            format("After stabilize, New Node State = ~p~n", [NewState]),
            chordProtocol(NewState);

        {updateFingerTable, NodeList, M} ->
            OwnIdentity = dict:fetch('OwnIdentity', State),
            SuccessorId = dict:fetch('SuccessorId', State),
            PredecessorId = dict:fetch('PredecessorId', State),
            UpdatedFingerTable = generateFingerTable(NodeList, OwnIdentity, M, lists:seq(1,M), []),
            SuccessorIndex = findNode(1, SuccessorId, NodeList),
            PredecessorIndex = findNode(1, PredecessorId, NodeList),
            Temp1 = dict:store('SuccessorIndex', SuccessorIndex, State),
            Temp2 = dict:store('PredecessorIndex', PredecessorIndex, Temp1),
            NewState = dict:store('FingerTable', UpdatedFingerTable, Temp2),
            format("After updating Finger Table = ~p~n", [NewState]),
            chordProtocol(NewState);

        {startMessaging, NumRequests, NodeList, M} ->
            FirstNode = lists:nth(1, NodeList),
            LastNode = lists:last(NodeList),
            OwnIdentity = dict:fetch('OwnIdentity', State),
            sendMessage(0, NumRequests, NodeList, M, FirstNode, LastNode, OwnIdentity, State),
            chordProtocol(State);

        {lookup, RandomNumber, Hops, M, FirstNode, LastNode} ->
            OwnIdentity = dict:fetch('OwnIdentity', State),
            lookup(RandomNumber, OwnIdentity, State, Hops, M, FirstNode, LastNode),
            chordProtocol(State)
    end.

sendMessage(I, NumRequests, _, _, _, _, _, _) when I =:= NumRequests ->
    ok;
sendMessage(I, NumRequests, NodeList, M, FirstNode, LastNode, OwnIdentity, State) ->
    timer:sleep(50),
    Hops = 0,
    RandomNumber = rand:uniform(LastNode),
    lookup(RandomNumber, OwnIdentity, State, Hops, M, FirstNode, LastNode),
    sendMessage(I+1, NumRequests, NodeList, M, FirstNode, LastNode, OwnIdentity, State).

lookup(RandomNumber, OwnIdentity, State, Hops, M, FirstNode, LastNode) ->
    SuccessorId = dict:fetch('SuccessorId', State),
    PredecessorId = dict:fetch('PredecessorId', State),

    if 
        (OwnIdentity =:= FirstNode) -> 
            if ((RandomNumber >= 0) and (RandomNumber =< OwnIdentity)) -> 
                    sendHopCount(Hops);
            true -> 
                if ((RandomNumber>OwnIdentity) and (RandomNumber =< SuccessorId)) -> 
                    sendHopCount(Hops+1);
                true -> 
                    forwardRequest(State, SuccessorId, RandomNumber, Hops+1, M, FirstNode, LastNode)
                end
            end;        
        true ->
            if ((RandomNumber > PredecessorId) and (RandomNumber =< OwnIdentity)) -> 
                    sendHopCount(Hops);
            true ->
                if ((RandomNumber>OwnIdentity) and (RandomNumber =< SuccessorId)) -> 
                    sendHopCount(Hops+1);
                true -> 
                    forwardRequest(State, SuccessorId, RandomNumber, Hops+1, M, FirstNode, LastNode)
                end
            end
        end.               

forwardRequest(State, SuccessorId, RandomNumber, Hops, M, FirstNode, LastNode) ->
    FingerTable = dict:fetch('FingerTable', State),
    ClosetNeighbour = findClosestNeighbour(1, RandomNumber, SuccessorId, FingerTable, M, []),
    integerToAtom(ClosetNeighbour) ! {lookup, RandomNumber, Hops, M, FirstNode, LastNode}.


findClosestNeighbour(I, RandomNumber, SuccessorId, NodeList, M, Neighbours) ->
    if 
        (I > M) ->
            if Neighbours == [] -> SuccessorId;
            true -> lists:max(Neighbours)
            end;
        true ->    
            Value = lists:nth(I, NodeList),
            if 
                (Value =< RandomNumber) ->
                    Temp = Neighbours ++ [Value],
                    findClosestNeighbour(I+1, RandomNumber, SuccessorId, NodeList, M, Temp);
                true ->
                    findClosestNeighbour(I+1, RandomNumber, SuccessorId, NodeList, M, Neighbours)
            end
    end.

        
sendHopCount(Hops) ->
    'project3' ! {receiveHops, Hops}.

findNode(I, Node, NodesList) ->
    Target = lists:nth(I, NodesList),
    if
        Target =:= Node -> I;
        true -> findNode(I+1, Node, NodesList)
    end.

getPredecessorData(NodeId, NodeList) ->
    PredecessorIndex = findNode(1, NodeId, NodeList)-2,
    PredecessorId = lists:nth(PredecessorIndex, NodeList),
    {PredecessorIndex, PredecessorId}.

createChordCircle(NodeList, M) ->
    lists:foreach(
        fun(Node) ->
            Index = element(1, Node),
            NodeName = element(2, Node),
            integerToAtom(NodeName) ! {initializeNode, Index, NodeList, M}
        end,
        lists:zip(lists:seq(1,length(NodeList)), NodeList)
    ).

integerToAtom(Value) ->
    list_to_atom(integer_to_list(Value)).

computeHashValue(I, M) ->
    Hash = crypto:hash(sha, integer_to_list(I)),
    EncodedHash = binary:bin_to_list(binary:encode_hex(Hash)),
    HashValue = list_to_integer(lists:sublist(EncodedHash, M), 16),
    HashValue.

generateHashes(I, _, HashList, NumNodes) when I =:= NumNodes+1 ->
    lists:sort(HashList);
generateHashes(I, M, HashList, NumNodes) ->
    NewHashList = lists:append(HashList, [computeHashValue(I, M)]),
    generateHashes(I+1, M, NewHashList, NumNodes).


createProcesses(NumNodes, M) ->
    HashList = generateHashes(1, M, [], NumNodes),
    Flag = length(lists:uniq(HashList)),
    if 
        Flag =:= NumNodes ->
            lists:foreach(
                fun(Hash) ->
                    Pid = spawn(project3, chordProtocol, [[]]),
                    register(integerToAtom(Hash), Pid)
                end,
                HashList
            ),
            {HashList, M*4};
        true ->
            createProcesses(NumNodes, M+1)
    end.

main(NumNodes, NumRequests) ->
    register(project3, self()),
    ProcessData = createProcesses(NumNodes, computeM(NumNodes)),
    NodeList = element(1, ProcessData),
    M = element(2, ProcessData),

    format("M = ~p Nodes = ~p~n", [M, NodeList]),

    RemoveData = removeRandomNode(NodeList),

    RemovedNode = element(1, RemoveData),
    NewNodeList = element(2, RemoveData),

    createChordCircle(NewNodeList, M),
    joinChord(RemovedNode, NewNodeList, lists:nth(1, NewNodeList)),
    stabilize(RemovedNode, NodeList),
    updateFingerTable(NodeList, M),

    startRequests(NodeList, NumRequests, M),
    TotalRequests = NumNodes * NumRequests,

    HopCount = checkHops(TotalRequests, 0, 0),

    format("Average Hop Count = ~p~n", [HopCount / TotalRequests]),

    lists:foreach(
            fun(Id) ->
                RegisteredPidName = integerToAtom(Id),
                Pid = whereis(RegisteredPidName),
                unregister(RegisteredPidName),
                exit(Pid, kill)
            end,
            NodeList
        ),
    unregister(project3).

computeM(NumNodes) ->
    Val = round(math:ceil(math:log2(NumNodes))),
    if 
        Val rem 4 =:= 0 ->
            (Val div 4);
        true ->
            (Val div 4) + 1
    end.

removeRandomNode(NodeList) ->
    [Head | TailList] = NodeList,
    Tail = lists:last(TailList),
    RestList = lists:droplast(TailList),
    Len = length(RestList),
    RemovedNode = lists:nth(rand:uniform(Len), RestList),
    NewNodeList = [Head] ++ lists:delete(RemovedNode, RestList) ++ [Tail],
    {RemovedNode, NewNodeList}.

joinChord(RemovedNodeId, NewNodeList, Head) ->
    integerToAtom(RemovedNodeId) ! {join, RemovedNodeId},
    integerToAtom(Head) ! {findSuccessor, RemovedNodeId, NewNodeList}.

stabilize(NodeId, NodeList) ->
    timer:sleep(1000),
    integerToAtom(NodeId) ! {stabilize, NodeList},
    timer:sleep(1000).

updateFingerTable(NodeList, M) ->
    lists:foreach(
        fun(Node) ->
            integerToAtom(Node) ! {updateFingerTable, NodeList, M}
        end,
        NodeList
    ).

startRequests(NodeList, NumRequests, M) ->
    lists:foreach(
        fun(Node) ->
            integerToAtom(Node) ! {startMessaging, NumRequests, NodeList, M}
        end,
        NodeList
    ).

checkHops(TotalRequests, CompletedRequests, TotalHops) when TotalRequests =:= CompletedRequests ->
    TotalHops;
checkHops(TotalRequests, CompletedRequests, TotalHops) ->
    receive
        {receiveHops, Hops} ->
            format("Number of requests left = ~p~n", [TotalRequests - CompletedRequests]),
            checkHops(TotalRequests, CompletedRequests +1, TotalHops + Hops)
    end.