# automerge-repo-demo

Project goal: add an integration layer between automerge and client code, compatible with any async runtime.

## Iteration 1

**What:** use an async public interface, and use async internally(but hidden from view).

**How:** traits for network and storage adapters are async, document handle method are async as well. Use a tokio runtime internally.

**Notes:** Need to run library inside a native thread, because tokio runtime cannot be dropped in an async context.

**Problems:** intermittent panics, mostly about channels being disconnected apparently without reason. 
Occured around use of document handles in client code that ran on a different tokio runtime. 

**Hypothesis:** panics arise from sharing synchronization primitives between different runtime contexts. 

## Iteration 2

**What:** test above hypothesis.

**How:** run everything, client and library code, in the context of a single tokio runtime. 

**Notes:** panics go away, but we now have a tokio demo app, which is not the goal of the project. 

**Hypothesis:** as a way of integrating with any runtime, use native threading internally, 
and offer a "normal" interface with guarantees about blocking. 

## Iteration 3

**What:** test above hypothesis.

**How:** Use a native thread to run event-loop of library. 

Offer a public interface that is not async, 
but guarantee non-blocking operation by using a pull-based workflow signalling backpressure(see [`sink_wants_events`](https://github.com/gterzian/automerge-repo-demo/blob/a3cccafd6df6c100171c8cc50e1ce1a836cae84d/src/interfaces.rs#L50)). 

Alternatively, document methods as blocking, 
and let client code deal with those using a `spawn_blocking`-like API from the runtime of their choice(see [`wait_ready`](https://github.com/gterzian/automerge-repo-demo/blob/a3cccafd6df6c100171c8cc50e1ce1a836cae84d/src/dochandle.rs#L70)). 

[main.rs](https://github.com/gterzian/automerge-repo-demo/blob/master/src/main.rs) contains the example "client code". 

Current state works pretty well, good basis for further discussion. 
