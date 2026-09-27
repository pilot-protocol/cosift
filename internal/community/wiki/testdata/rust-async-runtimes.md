## Execution model

An `async fn` in Rust returns a value that implements the `Future` trait. Calling it runs none of its body: the compiler turns the function into a state machine whose states are the points where it awaits, and the state machine advances only when something calls `poll` on it [1]. That something is an executor. A future that cannot make progress returns `Poll::Pending` and hands the executor a waker; when the resource it waits on becomes ready, the waker schedules the task to be polled again [2]. This design keeps futures cheap, because no thread and no stack are reserved for a task that is waiting.

The standard library defines `Future`, `Poll`, `Context` and `Waker`, but it ships no executor and no asynchronous input and output. A runtime supplies both. It pairs the executor with a reactor that registers sockets, files and timers with the operating system's readiness interface, such as epoll, kqueue or IOCP, and wakes the tasks whose resources became ready [1].

## Scheduling

Runtimes differ mainly in how they place tasks on threads. A single-threaded executor polls every task on the thread that runs it, which avoids synchronisation and suits small services and tests. A multi-threaded executor keeps a run queue per worker thread; an idle worker steals queued tasks from a busy one, which balances load without a central lock [3]. Tasks spawned on a multi-threaded runtime must be `Send`, because any worker may resume them after an await point.

Blocking inside a task stalls every other task on that worker. Long computations and blocking system calls therefore belong on a separate blocking pool, which runtimes expose as a dedicated spawn function, or on a thread of their own [2].

## Choosing a runtime

Libraries that perform input and output usually target one runtime's traits for sockets and timers, so the choice of runtime tends to follow the libraries an application uses. Code that only composes futures, with combinators or `select`-style macros, can stay runtime-agnostic. Cancellation is cooperative: dropping a future stops it at its last await point, so work that must finish, such as flushing a buffer, needs its own task or an explicit guard [2].

## Key facts

- An `async fn` compiles to a state machine that advances only when an executor polls it [1].
- A pending future registers a waker, and the waker reschedules the task when its resource is ready [2].
- The standard library defines the `Future` trait but provides no executor or reactor [1].
- Multi-threaded runtimes balance load by letting idle workers steal queued tasks [3].
- Blocking work belongs on a separate blocking pool, never inside an async task [2].
