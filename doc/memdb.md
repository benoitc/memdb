

# Module memdb #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

<a name="types"></a>

## Data Types ##




### <a name="type-db">db()</a> ###


<pre><code>
db() = #{}
</code></pre>




### <a name="type-fold_options">fold_options()</a> ###


<pre><code>
fold_options() = [{start_key, <a href="#type-key">key()</a>} | {end_key, <a href="#type-key">key()</a>} | {gt, <a href="#type-key">key()</a>} | {gte, <a href="#type-key">key()</a>} | {lt, <a href="#type-key">key()</a>} | {lte, <a href="#type-key">key()</a>} | {max, non_neg_integer()}]
</code></pre>




### <a name="type-iterator">iterator()</a> ###


<pre><code>
iterator() = pid()
</code></pre>




### <a name="type-iterator_ops">iterator_ops()</a> ###


<pre><code>
iterator_ops() = first | last | next | prev | binary()
</code></pre>




### <a name="type-iterator_options">iterator_options()</a> ###


<pre><code>
iterator_options() = [keys_only]
</code></pre>




### <a name="type-key">key()</a> ###


<pre><code>
key() = binary()
</code></pre>




### <a name="type-value">value()</a> ###


<pre><code>
value() = term() | any()
</code></pre>




### <a name="type-write_ops">write_ops()</a> ###


<pre><code>
write_ops() = [{put, <a href="#type-key">key()</a>, <a href="#type-value">value()</a>} | {delete, <a href="#type-key">key()</a>}]
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#close-1">close/1</a></td><td>close a database.</td></tr><tr><td valign="top"><a href="#code_change-3">code_change/3</a></td><td></td></tr><tr><td valign="top"><a href="#contains-2">contains/2</a></td><td>check if a Key exists in the database.</td></tr><tr><td valign="top"><a href="#delete-2">delete/2</a></td><td>delete a Key.</td></tr><tr><td valign="top"><a href="#find-2">find/2</a></td><td></td></tr><tr><td valign="top"><a href="#fold-4">fold/4</a></td><td>fold all K/Vs in the database with a function Fun.</td></tr><tr><td valign="top"><a href="#fold_keys-4">fold_keys/4</a></td><td></td></tr><tr><td valign="top"><a href="#get-2">get/2</a></td><td>returns the Value associated to the Key.</td></tr><tr><td valign="top"><a href="#get-3">get/3</a></td><td>returns the Value associated to the Key.</td></tr><tr><td valign="top"><a href="#handle_call-3">handle_call/3</a></td><td></td></tr><tr><td valign="top"><a href="#handle_cast-2">handle_cast/2</a></td><td></td></tr><tr><td valign="top"><a href="#handle_info-2">handle_info/2</a></td><td></td></tr><tr><td valign="top"><a href="#init-1">init/1</a></td><td></td></tr><tr><td valign="top"><a href="#iterator-1">iterator/1</a></td><td>initialize an iterator.</td></tr><tr><td valign="top"><a href="#iterator-2">iterator/2</a></td><td>initialize an iterator with options.</td></tr><tr><td valign="top"><a href="#iterator_close-1">iterator_close/1</a></td><td>close the iterator.</td></tr><tr><td valign="top"><a href="#iterator_loop-1">iterator_loop/1</a></td><td></td></tr><tr><td valign="top"><a href="#iterator_move-2">iterator_move/2</a></td><td>traverse the iterator using different operations.</td></tr><tr><td valign="top"><a href="#open-1">open/1</a></td><td>open the database Name.</td></tr><tr><td valign="top"><a href="#open-2">open/2</a></td><td></td></tr><tr><td valign="top"><a href="#put-3">put/3</a></td><td>Associates Key with value Value and store it.</td></tr><tr><td valign="top"><a href="#terminate-2">terminate/2</a></td><td></td></tr><tr><td valign="top"><a href="#write_batch-2">write_batch/2</a></td><td>Apply atomically a set of updates to the database.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="close-1"></a>

### close/1 ###

`close(X1) -> any()`

close a database

<a name="code_change-3"></a>

### code_change/3 ###

`code_change(OldVsn, State, Extra) -> any()`

<a name="contains-2"></a>

### contains/2 ###

<pre><code>
contains(Key::<a href="#type-key">key()</a>, Db::<a href="#type-db">db()</a>) -&gt; true | false
</code></pre>
<br />

check if a Key exists in the database

<a name="delete-2"></a>

### delete/2 ###

<pre><code>
delete(Key::<a href="#type-key">key()</a>, Db::<a href="#type-db">db()</a>) -&gt; ok
</code></pre>
<br />

delete a Key

<a name="find-2"></a>

### find/2 ###

`find(Key, Db) -> any()`

<a name="fold-4"></a>

### fold/4 ###

<pre><code>
fold(Fun::function(), AccIn::any(), Db::<a href="#type-db">db()</a>, Opts::<a href="#type-fold_options">fold_options()</a>) -&gt; AccOut::any()
</code></pre>
<br />

fold all K/Vs in the database with a function Fun.
Additionnaly you can pass the following options:

* 'gt', (greater than), 'gte' (greather than or equal): define the lower
bound of the range to fold. Only the records where the key is greater (or
equal to) will be given to the function.

* 'lt' (less than), 'lte' (less than or equal): define the higher bound
of the range to fold. Only the records where the key is less than (or equal
to) will be given to the function

* 'start_key', 'end_key', legacy to 'gte', 'lte'

* 'max' (default=0), the maximum of records to fold before returning the
resut

* 'fill_cache' (default is true): should be the data cached in
memory?


Example of function : Fun(Key, Value, Acc) -> Acc2 end.

<a name="fold_keys-4"></a>

### fold_keys/4 ###

`fold_keys(Fun, Acc0, Db, Opts0) -> any()`

<a name="get-2"></a>

### get/2 ###

<pre><code>
get(Key::<a href="#type-key">key()</a>, Db::<a href="#type-db">db()</a>) -&gt; Value::<a href="#type-value">value()</a>
</code></pre>
<br />

returns the Value associated to the Key. If the key doesn't exits and error will be raised.

<a name="get-3"></a>

### get/3 ###

<pre><code>
get(Key::<a href="#type-key">key()</a>, Db::<a href="#type-db">db()</a>, Default::any()) -&gt; Value::<a href="#type-value">value()</a>
</code></pre>
<br />

returns the Value associated to the Key. Default is returned if the Key doesn't exist.

<a name="handle_call-3"></a>

### handle_call/3 ###

`handle_call(Msg, From, State) -> any()`

<a name="handle_cast-2"></a>

### handle_cast/2 ###

`handle_cast(Msg, State) -> any()`

<a name="handle_info-2"></a>

### handle_info/2 ###

`handle_info(Info, State) -> any()`

<a name="init-1"></a>

### init/1 ###

`init(X1) -> any()`

<a name="iterator-1"></a>

### iterator/1 ###

<pre><code>
iterator(Db::<a href="#type-db">db()</a>) -&gt; <a href="#type-iterator">iterator()</a>
</code></pre>
<br />

initialize an iterator. And itterator allows you to iterrate over a consistent view of the database without
blocking any writes.

Note: compared to ETS you won't have to worry about the possibility that a key may be inserted while you iterrate.
The version you browsing won't be affected.

<a name="iterator-2"></a>

### iterator/2 ###

<pre><code>
iterator(Db::<a href="#type-db">db()</a>, Options::<a href="#type-iterator_options">iterator_options()</a>) -&gt; <a href="#type-iterator">iterator()</a>
</code></pre>
<br />

initialize an iterator with options. `keys_only` is the only option available for now.

<a name="iterator_close-1"></a>

### iterator_close/1 ###

<pre><code>
iterator_close(Iterator::<a href="#type-iterator">iterator()</a>) -&gt; ok
</code></pre>
<br />

close the iterator

<a name="iterator_loop-1"></a>

### iterator_loop/1 ###

`iterator_loop(Itr) -> any()`

<a name="iterator_move-2"></a>

### iterator_move/2 ###

<pre><code>
iterator_move(Iterator::<a href="#type-iterator">iterator()</a>, Op::<a href="#type-iterator_ops">iterator_ops()</a>) -&gt; {ok, Key::<a href="#type-key">key()</a>, Value::<a href="#type-value">value()</a>} | {ok, Key::<a href="#type-key">key()</a>} | '$iterator_limit' | {error, iterator_closed} | {error, invalid_iterator}
</code></pre>
<br />

traverse the iterator using different operations

<a name="open-1"></a>

### open/1 ###

<pre><code>
open(Name::atom()) -&gt; <a href="#type-db">db()</a>
</code></pre>
<br />

open the database Name

<a name="open-2"></a>

### open/2 ###

`open(Name, Options) -> any()`

<a name="put-3"></a>

### put/3 ###

<pre><code>
put(Key::<a href="#type-key">key()</a>, Value::<a href="#type-value">value()</a>, Db::<a href="#type-db">db()</a>) -&gt; ok
</code></pre>
<br />

Associates Key with value Value and store it.

<a name="terminate-2"></a>

### terminate/2 ###

`terminate(Reason, State) -> any()`

<a name="write_batch-2"></a>

### write_batch/2 ###

<pre><code>
write_batch(Ops::<a href="#type-write_ops">write_ops()</a>, Db::<a href="#type-db">db()</a>) -&gt; ok
</code></pre>
<br />

Apply atomically a set of updates to the database

