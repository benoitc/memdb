%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-module('memdb').

%% API exports
-export([find/2,
         get/2, get/3,
         put/3,
         delete/2,
         write_batch/2,
         contains/2,
         fold_keys/4,
         fold/4]).


-export([iterator/1, iterator/2,
         iterator_move/2,
         iterator_close/1]).

-export([open/1, open/2,
         close/1]).

%% private entry point (supervisor callback)
-export([ start_link/2 ]).


%% private ent private entry points (gen_server callbacks)
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3]).

%% private iterator loop entry point
-export([ iterator_loop/1 ]).

-define(DEFAULT_FOLD_OPTIONS, #{start_key => first,
                                end_key => nil,
                                gt => nil,
                                gte => nil,
                                lt => nil,
                                lte => nil,
                                max => 0}).


-type db() :: #{}.
-type iterator() :: pid().

-type key() :: binary().
-type value() :: term() | any().
-type write_ops() :: [{put, key(), value()} | {delete, key()}].
-type fold_options() :: [{start_key, key()}
                         | {end_key, key()}
                         | {gt, key()}
                         | {gte, key()}
                         | {lt, key()}
                         | {lte, key()}
                         | {max, non_neg_integer()}].

-type iterator_ops() :: first | last | next | prev | binary().
-type iterator_options() :: [keys_only].



%%====================================================================
%%
%% API functions
%%====================================================================
%%


find(Key, Db) ->
    case catch get_last(Key, Db) of
        not_found -> error;
        V -> {ok, V}
    end.

%% @doc returns the Value associated to the Key. If the key doesn't exits and error will be raised.
-spec get(Key::key(), Db :: db()) -> Value :: value().
get(Key, Db) ->
   get_last(Db, Key).

%% @doc returns the Value associated to the Key. Default is returned if the Key doesn't exist.
-spec get(Key :: key(), Db :: db(), Default :: any()) -> Value :: value().
get(Key, Db, Default) ->
    case memdb:find(Key, Db) of
        {ok, Val} -> Val;
        error -> Default
    end.

%% @doc Associates Key with value Value and store it.
-spec put(Key :: key(), Value :: value(), Db :: db()) -> ok.
put(Key, Value, Db) ->
    write_batch([{put, Key, Value}], Db).

%% @doc delete a Key
-spec delete(Key :: key(), Db :: db()) -> ok.
delete(Key, Db) ->
    write_batch([{delete, Key}], Db).

%% @doc Apply atomically a set of updates to the database
-spec write_batch(Ops :: write_ops(), Db :: db()) -> ok.
write_batch(Ops, #{ writer := W }) ->
    gen_server:call(W, {write, Ops}).

%% @doc check if a Key exists in the database
-spec contains(Key :: key(), Db :: db()) -> true | false.
contains(Key, #{ tab := Tab }) ->
    case ets:next(Tab, {r, prefix(Key)}) of
        '$end_of_table' -> false;
        {r, KeyBin} ->
            {Key, _, Type} = decode_key(KeyBin),
            Type =:= 1
    end.

fold_keys(Fun, Acc0, Db, Opts0) ->
    Itr = memdb:iterator(Db, [keys_only]),
    Opts = fold_options(Opts0, ?DEFAULT_FOLD_OPTIONS),
    do_fold(Itr, Fun, Acc0, Opts).

%% @doc fold all K/Vs in the database with a function Fun.
%% Additionnaly you can pass the following options:
%% <ul>
%% <li>'gt', (greater than), 'gte' (greather than or equal): define the lower
%% bound of the range to fold. Only the records where the key is greater (or
%% equal to) will be given to the function.</li>
%% <li>'lt' (less than), 'lte' (less than or equal): define the higher bound
%% of the range to fold. Only the records where the key is less than (or equal
%% to) will be given to the function</li>
%% <li>'start_key', 'end_key', legacy to 'gte', 'lte'</li>
%% <li>'max' (default=0), the maximum of records to fold before returning the
%% resut</li>
%% <li>'fill_cache' (default is true): should be the data cached in
%% memory?</li>
%% </ul>
%%
%% Example of function : Fun(Key, Value, Acc) -> Acc2 end.
-spec fold(Fun :: function(), AccIn :: any(), Db :: db(), Opts :: fold_options()) -> AccOut :: any().
fold(Fun, Acc0, Db, Opts0) ->
    Itr = memdb:iterator(Db, []),
    Opts = fold_options(Opts0, ?DEFAULT_FOLD_OPTIONS),
    do_fold(Itr, Fun, Acc0, Opts).



%% @doc initialize an iterator. And itterator allows you to iterrate over a consistent view of the database without
%% blocking any writes.
%%
%% Note: compared to ETS you won't have to worry about the possibility that a key may be inserted while you iterrate.
%% The version you browsing won't be affected.
-spec iterator(Db :: db()) -> iterator().
iterator(Db) ->
    iterator(Db, []).

%% @doc initialize an iterator with options. `keys_only' is the only option available for now.
-spec iterator(Db :: db(), Options :: iterator_options()) -> iterator().
iterator(#{ writer := W }, Options) ->
	gen_server:call(W, {new_iterator, Options}).


%% @doc traverse the iterator using different operations
-spec iterator_move(Iterator :: iterator(), Op :: iterator_ops()) ->
    {ok, Key :: key(), Value :: value()}
    | {ok, Key :: key()}
    | '$iterator_limit'
    | {error, iterator_closed}
    | {error, invalid_iterator}.
iterator_move(Itr, Op)
  when Op =:= first;
       Op =:= last;
       Op =:= next;
       Op =:= prev;
       is_binary(Op) ->

	Tag = erlang:monitor(process, Itr),

	catch Itr ! {Tag, self(), Op},
	receive
		{Tag, Reply} ->
			erlang:demonitor(Tag, [flush]),
			Reply;
		{'DOWN', Tag, _, Itr, _} ->
			{error, iterator_closed}
	after 5000 ->
			erlang:demonitor(Tag, [flush]),
			exit(timeout)
	end;
iterator_move(_, _) ->
	error(badarg).

%% @doc close the iterator
-spec iterator_close(Iterator :: iterator()) -> ok.
iterator_close(Itr) ->
    MRef = erlang:monitor(process, Itr),
    catch Itr ! {MRef, self(), close},
    receive
        {MRef, ok} ->
            erlang:demonitor(MRef, [flush]),
            ok;
        {'DOWN', MRef, _, _} -> ok
    after 5000 ->
              erlang:demonitor(MRef, [flush]),
              error(timeout)
    end.

%% @doc open the database Name
-spec open(atom()) -> db().
open(Name) ->
    open(Name, []).

open(Name, Options) when is_atom(Name) ->
    {ok, Pid} = start_link(Name, Options),
    #{ tab => Name, writer => Pid };
open(Name, _) ->
    error({invalid_name, Name}).

%% @doc close a database
close(#{ writer := Writer }) ->
    try
        gen_server:call(Writer, close, infinity)
    catch
        exit:{noproc,_} -> ok;
        exit:noproc -> ok;
        %%Handle the case where the monitor triggers
        exit:{normal, _} -> ok
    end.

%% @private
start_link(Name, Options) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Options], []).


%%====================================================================
%% Internal functions
%%====================================================================
%%
%%

get_last(#{ tab := Tab }, Key) ->
    case ets:next(Tab, {r, prefix(Key)}) of
        '$end_of_table' -> error(not_found);
        {r, KeyBin} ->
            {_Key, _Version, Type} = decode_key(KeyBin),
            if
                Type =:= 0 -> error(not_found); %% deleted
                true -> i_lookup(Tab, {r, KeyBin})
            end
    end.



i_lookup(Tab, Key) ->
    [{_, V}] = ets:lookup(Tab, Key),
    V.


%% private
do_fold(Itr, Fun, Acc0, #{gt := GT, gte := GTE}=Opts) ->
    {Start, Inclusive} = case {GT, GTE} of
                      {nil, nil} -> {first, true};
                      {first, _} -> {first, false};
                      {K, _} when is_binary(K) -> {K, false};
                      {_, K} -> {K, true}
                  end,

    try
        case memdb:iterator_move(Itr, Start) of
            {ok, Start} when Inclusive /= true ->
                fold_loop(memdb:iterator_move(Itr, next), Itr, Fun,
                          Acc0, 0, Opts);
            {ok, Start, _V} when Inclusive /= true ->
                fold_loop(memdb:iterator_move(Itr, next), Itr, Fun,
                          Acc0, 0, Opts);
            Next ->
                fold_loop(Next, Itr, Fun, Acc0, 0, Opts)

        end
    after
        memdb:iterator_close(Itr)
    end.

fold_loop('$iterator_limit', _Itr, _Fun, Acc, _N, _Opts) ->
	Acc;
fold_loop({error, iterator_closed}, _Itr, _Fun, Acc0, _N, _Opts) ->
    throw({iterator_closed, Acc0});
fold_loop({error, invalid_iterator}, _Itr, _Fun, Acc0, _N, _Opts) ->
    Acc0;
fold_loop({ok, K}=KO, Itr, Fun, Acc0, N0, #{lt := End}=Opts) when End /= nil, K < End ->
    fold_loop1(KO, Itr, Fun, Acc0, N0, Opts);
fold_loop({ok, K}=KO, Itr, Fun, Acc0, N0, #{lte := End}=Opts) when End /= nil orelse K =< End ->
    fold_loop1(KO, Itr, Fun, Acc0, N0, Opts);
fold_loop({ok, _K}=KO, Itr, Fun, Acc0, N,  #{lt := nil, lte := nil}=Opts) ->
    fold_loop1(KO, Itr, Fun, Acc0, N, Opts);
fold_loop({ok, _K}, _Itr, _Fun, Acc0, _N, _Opts) ->
    Acc0;
fold_loop({ok, K, _V}=KV, Itr, Fun, Acc0, N0, #{lt := End}=Opts) when End /= nil, K < End ->
    fold_loop1(KV, Itr, Fun, Acc0, N0, Opts);
fold_loop({ok, K, _V}=KV, Itr, Fun, Acc0, N0, #{lte := End}=Opts) when End /= nil, K =< End ->
    fold_loop1(KV, Itr, Fun, Acc0, N0, Opts);
fold_loop({ok, _K, _V}=KV, Itr, Fun, Acc0, N0, #{lt := nil, lte := nil}=Opts) ->
    fold_loop1(KV, Itr, Fun, Acc0, N0, Opts);
fold_loop({ok, _K, _V}, _Itr, _Fun, Acc0, _N, _Opts) ->
    Acc0.


fold_loop1({ok, K}, Itr, Fun, Acc0, N0, #{max := Max}=Opts) ->
    Acc = Fun(K, Acc0),
    N = N0 + 1,
    if ((Max =:=0) orelse (N < Max)) ->
            fold_loop(memdb:iterator_move(Itr, next), Itr, Fun, Acc, N, Opts);
        true ->
            Acc
	end;
fold_loop1({ok, K, V}, Itr, Fun, Acc0, N0, #{max := Max}=Opts) ->
    Acc = Fun(K, V, Acc0),
    N = N0 + 1,
    if
        ((Max =:= 0) orelse (N < Max)) ->
            fold_loop(memdb:iterator_move(Itr, next), Itr, Fun, Acc, N,  Opts);
        true ->
            Acc
    end.
fold_options([], Options) ->
    Options;
fold_options([{start_key, Start} | Rest], Options) when is_binary(Start) or (Start =:= first) ->
    fold_options(Rest, Options#{gte => Start});
fold_options([{end_key, End} | Rest], Options) when is_binary(End) or (End == nil) ->
    fold_options(Rest, Options#{lte=>End});
fold_options([{gt, GT} | Rest], Options) when is_binary(GT) or (GT =:= first) ->
    fold_options(Rest, Options#{gt=>GT});
fold_options([{gte, GT} | Rest], Options) when is_binary(GT) or (GT =:= first) ->
    fold_options(Rest, Options#{gte=>GT});
fold_options([{lt, LT} | Rest], Options) when is_binary(LT) or (LT == nil) ->
    fold_options(Rest, Options#{lt=>LT});
fold_options([{lte, LT} | Rest], Options) when is_binary(LT) or (LT == nil) ->
    fold_options(Rest, Options#{lte=>LT});
fold_options([{max, Max} | Rest], Options) ->
    fold_options(Rest, Options#{max=>Max});
fold_options([_ | Rest], Options) ->
    fold_options(Rest, Options).


%%====================================================================
%% gen_server functions
%%====================================================================
%%
%%


init([Name, _Options]) ->
    process_flag(trap_exit, true),
    case ets:info(Name, name) of
        undefined ->
            ets:new(Name, [ordered_set, public, named_table,
                           {read_concurrency, true},
                           {write_concurrency, true}]);
        _ ->
            ok
    end,
    Version = case ets:insert_new(Name, {'$version', 0}) of
                  true -> 0;
                  false ->
                      ets:update_counter(Name, '$version', {2, 0})
              end,

    {ok, #{tab => Name,
           version => Version,
           iterator => nil,
           iterators => #{},
           busy_versions => []}}.


handle_call({write, Ops}, _From, State) ->
    #{ tab := Tab, version := Version} = State,
    NextVersion = Version + 1,
    ToInsert = process_ops(Ops, NextVersion, Tab, []),
    %% the operations is atomic,
    ets:insert(Tab, ToInsert),
    {reply, ok, State#{ version => NextVersion }};

handle_call({new_iterator, Options}, _From, State) ->
    {Itr, NewState} = spawn_iterator(State, Options) ,
    {reply, Itr, NewState};

handle_call(close, _From, #{ tab := Tab } = State) ->
    ets:delete(Tab),
    {stop, normal, ok, State};

handle_call(_Msg, _From, State) ->
    {reply, badarg, State}.

handle_cast({write, Ops}, State) ->
    #{ tab := Tab, version := Version} = State,
    NextVersion = Version +1,
    ToInsert = process_ops(Ops, NextVersion, Tab, []),
    ets:insert(Tab, ToInsert),
    {noreply, State#{ version => NextVersion }};

handle_cast({close_iterator, Itr}, State) ->
    catch exit(Itr, normal),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, _}, State) ->
    #{ iterators := Iterators, locks_versions := Locks} = State,
    Iterators2 = maps:remove(Pid, Iterators),
    Locks2 = case maps:find(Pid, Iterators) of
                 {ok, Version} -> unlock_version(Version, Locks);
                 error -> Locks
             end,
    {noreply, State#{iterators => Iterators2, locks_versions => Locks2}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
      {ok, State}.

process_ops([{put, Key, Value} | Rest], Version, Tab, Acc) ->
    Meta = case ets:lookup(Tab, {k, Key}) of
               [] -> {Version, Version, 1};
               [{_, {XMin, _, _}}] -> {XMin, Version, 1}
           end,

    Acc2 = [{{r, make_key(Key, Version, 1)}, Value},
            {{k, Key}, Meta}| Acc],
    process_ops(Rest, Version, Tab, Acc2);
process_ops([{delete, Key} | Rest], Version, Tab, Acc) ->
    case ets:lookup(Tab, {k, Key}) of
        [] ->
            process_ops(Rest, Version, Tab, Acc);
        [{_, {XMin, _, _}}] ->
            Acc2 = [{{r, make_key(Key, Version, 0)}, <<>>},
                    {{k, Key}, {XMin, Version, 0}}],
            process_ops(Rest, Version, Tab, Acc2)
    end;
process_ops([], Version, _Tab, Acc) ->
    lists:reverse([{'$version', Version} | Acc]).


spawn_iterator(State, Options) ->
    #{ version := Version,
       iterator := Itr,
       iterators := Iterators,
       busy_versions := Locks } = State,

    KeysOnly = proplists:get_value(keys_only, Options, false),

    {Itr2, State2} = maybe_init_iterator(Itr, State),
    Pid = spawn_link(?MODULE, iterator_loop, [Itr2#{ keys_only => KeysOnly }]),
    Iterators2 = Iterators#{ Pid => Version},
    Locks2 = lock_version(Version, Locks),
    NewState = State2#{ iterators => Iterators2,
                        locks_versions => Locks2 },
    {Pid, NewState}.

maybe_init_iterator(nil, State) ->
    init_iterator(State);
maybe_init_iterator(#{version := V}=Itr, #{ version := V}=State) ->
    {Itr, State};
maybe_init_iterator(_, State) ->
    init_iterator(State).

init_iterator(#{ tab := Tab, version := Version}=State) ->
    Itr = #{ tab => Tab,
             version => Version,
             dir => fwd,
             next => nil},
    {Itr, State#{ iterator => Itr }}.


iterator_loop(Itr) ->
    receive
        {Ref, From, close} ->
            catch From ! {Ref, ok};
        {Ref, From, first} ->
            {Reply, Itr2} = iterator_first(Itr),
            From ! {Ref, Reply},
            iterator_loop(Itr2);
        {Ref, From, last} ->
            {Reply, Itr2} = iterator_last(Itr),
            From ! {Ref, Reply},
            iterator_loop(Itr2);
        {Ref, From, next} ->
            {Reply, Itr2} = iterator_next(Itr),
            From ! {Ref, Reply},
            iterator_loop(Itr2);
        {Ref, From, prev} ->
            {Reply, Itr2} = iterator_prev(Itr),
            From ! {Ref, Reply},
            iterator_loop(Itr2);
        {Ref, From, Key} when is_binary(Key) ->
            {Reply, Itr2} = iterator_next_key(Key, Itr),
            From ! {Ref, Reply},
            iterator_loop(Itr2)
    end.


iterator_first(Itr) ->
    iterator_next(Itr#{ dir => fwd, next => nil }).

iterator_last(Itr) ->
    {Reply, Itr2} = iterator_prev(Itr#{ dir => rev, last => nil, next => nil }),
    {Reply, Itr2#{ dir => fwd }}.


iterator_lookup('$end_of_table', Itr) ->
    {'$iterator_limit', Itr#{ last => '$iterator_limit' }};
iterator_lookup({[{Key, _XMax}], Cont}, #{ keys_only := true }=Itr) ->
    {{ok, Key}, Itr#{ last => Key, next => Cont }};
iterator_lookup({[{Key, XMax}], Cont}, #{ tab := Tab, version := Version}=Itr) ->
    DbVersion = erlang:min(Version, XMax),
    case ets:lookup(Tab, {r, make_key(Key, DbVersion, 1)}) of
        [] -> error({error, invalid_iterator});
        [{_, Value}] ->
            {{ok, Key, Value}, Itr#{ last => Key, next => Cont }}
    end.



iterator_next(#{ tab := Tab, version := Version, next := nil } = Itr) ->
    Next = ets:select(Tab, [{{{k, '$1'}, {'$2', '$3', '$4'}},
                             [{'andalso',
                               {'=<', '$2', Version},
                               {'==', '$4', 1}}],
                             [{{'$1', '$3'}}]}], 1),
    iterator_lookup(Next, Itr#{ dir => fwd} );

iterator_next(#{ dir := fwd, next := Cont }=Itr) ->
    Next = ets:select(Cont),
    iterator_lookup(Next, Itr);
iterator_next(#{ dir := fwd, last := '$iterator_limit' }=Itr) ->
    {'$iterator_limit', Itr};
iterator_next(#{ last := '$iterator_limit' }=Itr) ->
    iterator_next(Itr#{ dir => fwd, last => nil, next => nil });
iterator_next(#{ tab := Tab, version := Version, last := Last }=Itr) ->
    Next = ets:select(Tab, [{{{k, '$1'}, {'$2', '$3', '$4'}},
                             [{'andalso',
                               {'>', '$1', Last },
                               {'=<', '$2', Version},
                               {'==', '$4', 1}}],
                             [{{'$1', '$3'}}]}], 1),
    iterator_lookup(Next, Itr#{ dir => fwd} ).

iterator_prev(#{ tab := Tab, version := Version, next := nil } = Itr) ->
    Prev = ets:select_reverse(Tab, [{{{k, '$1'}, {'$2', '$3', '$4'}},
                                     [{'andalso',
                                       {'=<', '$2', Version},
                                       {'==', '$4', 1}}],
                                     [{{'$1', '$3'}}]}], 1),
    iterator_lookup(Prev, Itr#{ dir => rev} );
iterator_prev(#{ dir := rev, next := Cont }=Itr) ->
    Prev = ets:select(Cont),
    iterator_lookup(Prev, Itr);
iterator_prev(#{ dir := rev, last := '$iterator_limit' }=Itr) ->
    {'$iterator_limit', Itr};
iterator_prev(#{ last := '$iterator_limit' }=Itr) ->
    iterator_prev(Itr#{ dir => rev, last => nil, next => nil });
iterator_prev(#{ tab := Tab, version := Version, last := Last }=Itr) ->
    Prev = ets:select_reverse(Tab, [{{{k, '$1'}, {'$2', '$3', '$4'}},
                                     [{'andalso',
                                       {'<', '$1', Last },
                                       {'=<', '$2',  Version},
                                       {'==', '$4',  1}}],
                                     [{{'$1', '$3'}}]}], 1),
    iterator_lookup(Prev, Itr#{ dir => fwd} ).


iterator_next_key(Key, #{ tab := Tab, version := Version} = Itr) ->
    Next = ets:select(Tab, [{{{k, '$1'}, {'$2', '$3', '$4'}},
                             [{'andalso',
                               {'>=', '$1', Key },
                               {'=<', '$2', Version},
                               {'==', '$4', 1}}],
                             [{{'$1', '$3'}}]}], 1),
    iterator_lookup(Next, Itr#{ dir => fwd} ).

lock_version(Version, Locks) ->
    Count = case lists:keyfind(Version, 1, Locks) of
                false -> 0;
                {Version, C} -> C
            end,
    lists:keyreplace(Version, 1, Locks, {Version, Count + 1}).

unlock_version(Version, Locks) ->
    case lists:keyfind(Version, 1, Locks) of
        false -> Locks;
        {Version, Count} ->
            Count2 = Count - 1,
            if
                Count2 >= 1 ->
                    lists:keyreplace(Version, 1, Locks, {Version, Count2});
                true ->
                    lists:keydelete(Version, 1, Locks)
            end
    end.


prefix(Key) ->
    << Key/binary, 16#ff >>.

make_key(Key, Version, Type) when is_binary(Key) ->
    << Key/binary, 16#FF, Type, (-Version):4/little-signed-integer-unit:8>>.

decode_key(Bin) when is_binary(Bin) ->
    case binary:split(Bin, << 16#ff >>) of
        [Key, << Type, Version:4/little-signed-integer-unit:8 >>]  ->
            {Key, -Version, Type};
        _ ->
            error(badkey)
    end.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").


basic_test() ->
    Db = memdb:open(test),
    memdb:put(<<"a">>, 1, Db),
    ?assertEqual(1, memdb:get(<<"a">>, Db)),
    memdb:put(<<"a">>, 2, Db),
    ?assertEqual(2, memdb:get(<<"a">>, Db)),
    memdb:put(<<"a1">>, 3, Db),
    ?assertEqual(3, memdb:get(<<"a1">>, Db)),
    ?assertEqual(2, memdb:get(<<"a">>, Db)),
    memdb:delete(<<"a">>, Db),
    ?assertError(not_found, memdb:get(<<"a">>, Db)),
    ?assertEqual(3, memdb:get(<<"a1">>, Db)),
    memdb:close(Db).

write_batch_test() ->
    Db = memdb:open(test),
    memdb:write_batch([{put, <<"a">>, 1},
                             {put, <<"b">>, 2},
                             {put, <<"c">>, 1}], Db),


    ?assert(memdb:contains(<<"a">>, Db)),
    ?assert(memdb:contains(<<"b">>, Db)),
    ?assert(memdb:contains(<<"c">>, Db)),
    ?assertEqual(1, memdb:get(<<"a">>, Db)),
    ?assertEqual(2, memdb:get(<<"b">>, Db)),
    ?assertEqual(1, memdb:get(<<"c">>, Db)),
    memdb:close(Db).


iterator_test() ->
    Db = memdb:open(test),
    memdb:write_batch([{put, <<"a">>, 1},
                             {put, <<"b">>, 2},
                             {put, <<"c">>, 1}], Db),

	Iterator = memdb:iterator(Db),
	memdb:put(<<"a">>, 2, Db),
	?assertEqual(2, memdb:get(<<"a">>, Db)),
    ?assertEqual({ok, <<"a">>, 1}, memdb:iterator_move(Iterator, next)),
    ?assertEqual({ok, <<"b">>, 2}, memdb:iterator_move(Iterator, next)),
    ?assertEqual({ok, <<"c">>, 1}, memdb:iterator_move(Iterator, next)),
    ?assertEqual('$iterator_limit', memdb:iterator_move(Iterator, next)),
    ?assertEqual('$iterator_limit', memdb:iterator_move(Iterator, next)),

    ?assertEqual({ok, <<"c">>, 1}, memdb:iterator_move(Iterator, prev)),
    ?assertEqual({ok, <<"b">>, 2}, memdb:iterator_move(Iterator, prev)),
    ?assertEqual({ok, <<"a">>, 1}, memdb:iterator_move(Iterator, prev)),
    ?assertEqual('$iterator_limit', memdb:iterator_move(Iterator, prev)),
    ?assertEqual('$iterator_limit', memdb:iterator_move(Iterator, prev)),
    ?assertEqual({ok, <<"a">>, 1}, memdb:iterator_move(Iterator, next)),





	?assertEqual({ok, <<"a">>, 1}, memdb:iterator_move(Iterator, first)),
	?assertEqual({ok, <<"c">>, 1}, memdb:iterator_move(Iterator, last)),

    ?assertEqual({ok, <<"b">>, 2}, memdb:iterator_move(Iterator, <<"b">>)),
    ?assertEqual({ok, <<"c">>, 1}, memdb:iterator_move(Iterator, next)),

    memdb:iterator_close(Iterator),

    ?assertEqual({error, iterator_closed}, memdb:iterator_move(Iterator, next)),

	Iterator2 = memdb:iterator(Db),
    ?assertEqual({ok, <<"a">>, 2}, memdb:iterator_move(Iterator2, next)),
    ?assertEqual({ok, <<"b">>, 2}, memdb:iterator_move(Iterator2, next)),
    ?assertEqual({ok, <<"c">>, 1}, memdb:iterator_move(Iterator2, next)),
    ?assertEqual('$iterator_limit', memdb:iterator_move(Iterator2, next)),
	memdb:iterator_close(Iterator2),

    memdb:close(Db).


fold_keys_test() ->
	Db = memdb:open(test),
	ok =  memdb:write_batch([{put, <<"a">>, 1},
								   {put, <<"b">>, 2},
								   {put, <<"c">>, 3},
								   {put, <<"d">>, 4}], Db),

	AccFun = fun(K, Acc) -> [K | Acc] end,
	?assertMatch([<<"a">>, <<"b">>, <<"c">>, <<"d">>],
				 lists:reverse(memdb:fold_keys(AccFun, [], Db, []))),
	memdb:close(Db).


fold_gt_test() ->
	Db = memdb:open(test),
	ok =  memdb:write_batch([{put, <<"a">>, 1},
								   {put, <<"b">>, 2},
								   {put, <<"c">>, 3},
								   {put, <<"d">>, 4}], Db),

	AccFun = fun(K, V, Acc) ->
					 [{K, V} | Acc]
			 end,

	?assertMatch([{<<"b">>, 2}, {<<"c">>, 3}, {<<"d">>, 4}],
				 lists:reverse(memdb:fold(AccFun, [], Db,[{gt, <<"a">>}]))),
	memdb:close(Db).


fold_lt_test() ->
	Db = memdb:open(test),

	ok =  memdb:write_batch([{put, <<"a">>, 1},
								   {put, <<"b">>, 2},
								   {put, <<"c">>, 3},
								   {put, <<"d">>, 4}], Db),

	AccFun = fun(K, V, Acc) ->
					 [{K, V} | Acc]
			 end,

	?assertMatch([{<<"a">>, 1}, {<<"b">>, 2}, {<<"c">>, 3}],
				 lists:reverse(memdb:fold(AccFun, [], Db, [{lt, <<"d">>}]))),
	memdb:close(Db).

fold_lt_gt_test() ->
	Db = memdb:open(test),
	ok =  memdb:write_batch([{put, <<"a">>, 1},
								   {put, <<"b">>, 2},
								   {put, <<"c">>, 3},
								   {put, <<"d">>, 4}], Db),

	AccFun = fun(K, V, Acc) ->
					 [{K, V} | Acc]
			 end,

	?assertMatch([{<<"b">>, 2}, {<<"c">>, 3}],
				 lists:reverse(memdb:fold(
								 AccFun, [], Db,
								 [{gt, <<"a">>},  {lt, <<"d">>}]))),
	memdb:close(Db).


fold_lt_gt_max_test() ->
	Db = memdb:open(test),
	ok =  memdb:write_batch([{put, <<"a">>, 1},
								   {put, <<"b">>, 2},
								   {put, <<"c">>, 3},
								   {put, <<"d">>, 4}], Db),



	AccFun = fun(K, V, Acc) ->
					 [{K, V} | Acc]
			 end,

	?assertMatch([{<<"b">>, 2}],
				 memdb:fold(AccFun, [], Db,  [{gt, <<"a">>},
													{lt, <<"d">>},
													{max, 1}])),
	memdb:close(Db).



-endif.
