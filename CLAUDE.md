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
- **Cancellation:** dropping the last strong ref cancels: `release_resources` drops every awaited child
  (the `awaited_child` list; one entry per in-flight co_await, there can be several in one expression)
  and the all_of/any_of shared states drop their children.
  `cancelable_task` stops at its next suspension point; non-cancelable tasks pin themselves
  (`m_self_handle`) when awaited and run to completion. any_of losers follow these rules and are
  destroyed only once they have stopped.
- **Hand-off rule ([expr.await]/5.1):** once `await_suspend` has published the coroutine, it may be
  resumed and finish on another thread before `await_suspend` returns. Never touch the awaiter,
  promise or frame after the hand-off. Never call `handle.done()` after `resume()` returns.
- **Finishing:** `final_awaiter` only stashes the continuation and pin in a thread_local;
  `promise_base::resume_and_finish` releases the resumer's strong ref and the pin *before* running
  the continuation. The parent must be the last owner when it is resumed, so that the child is destroyed
  before the parent can reuse the child's HALO slot. This includes *weak* references (the frame dies at
  weak count 0): never let a temporary weak handle to the child outlive the resumption, e.g. write
  `auto strong = weak.lock(); run(std::move(strong));`, not `run(std::exchange(weak, {}).lock())`.
- **Inline resumption:** `resume_promise_weak` called on a worker of the same context and pool (same
  hint) does not enqueue; it stores the weak handle in the thread_local `t_next`, and
  `run_on_this_thread` resumes it after the current coroutine has suspended/finished. A loop, not nested
  `resume()`, so the stack depth is constant. One slot only; anything else is enqueued. No fairness cap
  (deliberate).
- **HALO slots:** an elided frame lives in a slot of the enclosing scope, which is reused on the next
  loop iteration or after the scope ends. Anything that can outlive its co_await (any_of losers,
  interrupt_by arguments) must not be elided: `any_of` returns the non-elidable `any_of_task`.
  `any_of_once` keeps HALO and is only valid at the outermost scope of a coroutine body, never in a
  loop. `force_await_embed_in_current_frame` must never be used inside a loop.
- **Owner references only where needed:** a frame that is never elided (`any_of_task`,
  `set_never_elided`) must not hold its owner (`set_owner_if_needed`), otherwise its long-lived losers
  keep an elided owner's frame alive past the owner's slot lifetime.
- **Releases are iterative but HALO-aware:** `pin_resource_base::run_or_defer` queues only objects that
  hold their owner; owner-less objects are released immediately (they may live inside the frame whose
  destruction is releasing them). Keep this distinction if you touch the release path.
- **Context shutdown:** `lock()`/`unlock()` (co_spawn roots) are a counter that never blocks; the
  destructor waits for it to reach zero, then for the pending-job counter.

## Conventions

- Code lives in `coroutine::_details`; public names are re-exported with `using` at namespace scope.
- Keep the existing style: 4-space indent, `m_` members, comments explain *why* (lifetimes, races).
