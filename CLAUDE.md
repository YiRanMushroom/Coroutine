# CLAUDE.md

Header-only C++23 coroutine library (`coroutine.hpp`) on top of `thread_pool/` (dp::thread_pool, MIT).
`main.cpp` is a stress-test driver. See `CONTEXT.md` for the history of findings and open issues.

## Build & test

Clang is the primary compiler (HALO attributes are Clang-only). On this machine:

```sh
# release-ish build
"/c/Program Files/LLVM/bin/clang++" -std=c++23 -O2 -g main.cpp -o o2.exe
# AddressSanitizer build (runtime DLL must be on PATH)
"/c/Program Files/LLVM/bin/clang++" -std=c++23 -O1 -g -fsanitize=address -fno-omit-frame-pointer main.cpp -o asan.exe
PATH="/c/Program Files/LLVM/lib/clang/22/lib/windows:$PATH" ./asan.exe
```

CMake/CLion profiles also exist (`cmake-build-*`), including clang-21 and MSVC.

Test driver: `o2.exe [filter...]`. Arguments are substring filters, `-name` excludes. Tests named
`unsafe_*` only run when selected explicitly (they demonstrate misuse and are expected to crash).
Every test checks results and that no promise is leaked after its execution context is destroyed.

Always test optimized builds: HALO only happens with optimization, and most lifetime bugs found so far
only reproduce at -O1/-O2. Race bugs are intermittent; loop a test (e.g. 40 runs) before trusting it.

## Design invariants (do not break)

- **Ownership:** strong refs keep a coroutine running, weak refs keep its frame memory. Parents hold
  children strongly; children hold parents only weakly (`m_owner_handle`), released *after* the
  child's frame is destroyed (HALO: the child's frame may live inside the owner's frame).
- **Cancellation:** dropping the last strong ref cancels: `release_resources` runs every `cancel_hook`
  the coroutine is suspended on (an `awaited_child` drops the child; a `delay` removes its timer; a
  `detach_awaiter` cuts its link) and the all_of/any_of shared states drop their children. There can be
  several hooks at once (several co_awaits in one expression).
  `cancelable_task` stops at its next suspension point; non-cancelable tasks pin themselves
  (`m_self_handle`) when awaited and run to completion. any_of losers follow these rules and are
  destroyed only once they have stopped.
- **Hand-off rule ([expr.await]/5.1):** once `await_suspend` has published the coroutine, it may be
  resumed and finish on another thread before `await_suspend` returns. Never touch the awaiter,
  promise or frame after the hand-off. Never call `handle.done()` after `resume()` returns.
- **Parents wait for destruction, not completion (the core HALO rule):** a parent continues past
  `co_await child` only once the child has been *destroyed*. A finished child can still be referenced
  for a while: e.g. a thread that resumed it earlier has not returned from `resume()` yet ("stale
  resumer") while another thread already finished it. So `task_base::by_promise`: the child's
  continuation moves the result into the awaiter and releases the parent's reference
  (`awaited_child::release_child`, arbitrated with cancellation by an atomic flag); the child's
  `on_destroyed` hook resumes the parent. all_of and any_of do the same for all children
  (`wait_children_destroyed`). `final_awaiter` stashes the continuation and pin in a thread_local;
  `resume_and_finish` runs the continuation (while still holding the child) and then releases.
- **No lingering references to whoever you wake:** continuations and hooks reach the coroutine they wake
  only through a weak handle that they hand over (`schedule_resume`, or `combinator_sync::waiter` moved
  out by the single waker). Combinator children refer to the shared `combinator_sync` block, never to the
  combinator's frame. Also write `auto strong = weak.lock(); run(std::move(strong));`, not
  `run(std::exchange(weak, {}).lock())` (the temporary would live through the call).
- **Inline resumption:** `resume_promise_weak` called on a worker of the same context and pool (same
  hint) does not enqueue; it stores the weak handle in the thread_local `t_next`, and
  `run_on_this_thread` resumes it after the current coroutine has suspended/finished. A loop, not nested
  `resume()`, so the stack depth is constant. One slot only; anything else is enqueued. No fairness cap
  (deliberate). The loop checks `t_next` even when it was given nothing to resume: releasing a reference
  can destroy a coroutine whose on_destroyed hook schedules another one (a lost wakeup otherwise).
- **Structured concurrency (this is what makes HALO safe):** an elided frame lives in a slot of the
  enclosing scope, reused on the next loop iteration or after the scope ends. So nothing a coroutine
  starts may outlive the co_await that started it. Direct co_await and all_of satisfy this naturally;
  `any_of` (tuple and vector) waits after the winner until every child has been destroyed
  (`on_destroyed` hooks + `wait_children_destroyed`, counter starts at children + 1). Non-cancelable
  losers therefore delay any_of until they finish; `detach(task)` is the explicit opt-out (the task
  becomes a heap root like co_spawn; cancelling the returned task does not wait for it).
  `force_await_embed_in_current_frame` must never be used inside a loop (one slot per loop body).
- **Waiting without threads:** `sleep_for`, `poll_until`, `from_future`, `wait_for_semaphore` use the
  context's timer (`add_timer` / `cancel_timer`, a timer thread over a `std::map`), never block a
  thread, and are cancelable. Pending timers count as pending jobs. On Windows the timer resolution is
  ~15.6 ms by default, so short delays round up.
- **Releases are iterative but HALO-aware:** `pin_resource_base::run_or_defer` queues only objects that
  hold their owner; owner-less objects are released immediately (they may live inside the frame whose
  destruction is releasing them). Keep this distinction if you touch the release path.
- **destroy() order:** destroy the frame, then release the owner, then run `on_destroyed`.
- **Context shutdown:** `lock()`/`unlock()` (co_spawn roots) are a counter that never blocks; the
  destructor waits for it to reach zero, then for the pending-job counter.

## Conventions

- Code lives in `coroutine::_details`; public names are re-exported with `using` at namespace scope.
- Keep the existing style: 4-space indent, `m_` members, comments explain *why* (lifetimes, races).
