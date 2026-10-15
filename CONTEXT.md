# CONTEXT.md: review findings (2026-10-15)

Review and cleanup of `coroutine.hpp`, verified with the stress tests in `main.cpp` (Clang 22, -O2 and
ASan). "Baseline" means the code at commit `fa802f7`.

## 1. The semaphore in `suspend_and_then_t`

The old awaiter did `callback(h); m_semaphore.release();` in `await_suspend` and `m_semaphore.acquire()`
in `await_resume`.

- Standard: per [expr.await]/5.1 the coroutine is already suspended when `await_suspend` starts, so once
  the callback starts the children, another thread may resume the coroutine before `await_suspend`
  returns.
- What it protected: in `any_of`, the first finished child can resume the parent while the callback is
  still starting the remaining children (from raw `promise_base*`). The parent then drops the children
  (`non_throw_tasks = {}`) and the callback touches freed promises. `all_of` does not have this problem.
- Why it was the wrong fix: `release()` itself touches the awaiter after the hand-off (its lifetime may
  already have ended), it blocks a worker thread, and it does nothing about the `handle.done()` race in
  the resumer.
- Replacement: `start_children` takes a weak reference to every child before starting any of them, and
  starts them via `resume_promise_weak`. Children released early simply fail to `lock()` and never run,
  which is the intended any_of cancellation. `suspend_and_then_t` moves the callback onto the stack
  before calling it. The semaphore is gone.

## 2. Bugs found (all fixed unless noted)

| # | Bug | Evidence |
|---|-----|----------|
| 1 | **HALO slot reuse, direct co_await in a loop.** A finishing child resumed its parent *before* releasing the pin and the resumer's strong ref; the parent reused the child's elided slot for the next child while the old child was still touching it, so refcounts were corrupted. | Baseline `await_in_loop` crashes 3/3 and `any_of_cancels_loser` crashes intermittently. Fixed by `final_awaiter` + `resume_and_finish` (release first, continue last). |
| 2 | **HALO slot reuse, any_of losers.** Losers outlive any_of; if they (or the any_of frame) are elided into the caller, the caller reuses the slot. Inherent to "losers run on" + HALO (Rust has the same constraint: inline futures are cancelled on drop, anything that keeps running must be `spawn`ed onto the heap). | Baseline `any_of_in_loop` crashes 3/3. Fixed by splitting: `any_of` (non-elidable `any_of_task`, safe in loops) and `any_of_once` (HALO, outermost scope only). `unsafe_any_of_once_in_loop` demonstrates misuse. |
| 3 | **vector all_of/any_of used `force_await_embed_in_current_frame` in a loop**, so all wrappers shared one elided frame. | Baseline `all_of_vector` / `any_of_vector_fanout` crash every run at -O1/-O2 (`__debugbreak`, exit code 3). The embedding did not save allocations there anyway (120200 with and without). |
| 4 | **generator never resumed its consumer:** the continuation locked the moved-from `_weak_parent`. | Baseline `generator` hangs. |
| 5 | **tuple any_of continuation:** missing `return` when the parent is gone, so it dereferenced null. | Code reading; rewritten. |
| 6 | **tuple all_of/any_of read `tasks.get_promise()` after `std::move(tasks)`** in `x.with_context(tasks...)`. It works only because Clang evaluates the argument before the explicit object argument; C++17 sequencing requires the opposite. | Probe program: Clang prints 1 (should be 0). Contexts are now read before the move. |
| 7 | **tuple all_of return type** used `all_of_args_shared_state<Tasks...>` while the body cast to `<value_type...>` (different promise type, worked by layout accident). | Code reading. |
| 8 | **Resumer called `handle.done()` after `resume()`**, a precondition violation and data race when the coroutine was handed to another thread; `m_finalized` papered over the double finalization. | Removed; finishing now happens through `final_awaiter`. |
| 9 | **`block_on`** released a semaphore on its own stack from a worker (UAF window, hidden by `wait_idle()` + `NO_ASAN`). | Replaced by `std::promise`/`std::future` (refcounted shared state). |
| 10 | **`wait_idle()` called `clear_tasks()`**, which can drop pending resumptions; it also ignored the I/O pool. Destroying the context could race with jobs that enqueue into an already destroyed pool. | A pending-job counter now drives `wait_idle()` and the destructor drains it before the pools go away. |
| 11 | all_of/any_of over an empty vector never resumed. | Early `co_return {}`. |
| 12 | **Two co_awaits in one expression** (`co_await a() + co_await b()`): a promise had a single `m_child_handle` slot, filled in `by_promise`. The operands are unsequenced; MSVC runs both await_transforms first and even suspends for `b` before `a`'s await_resume. Also, a child pinned in `by_promise` leaked if the expression was abandoned by an exception before suspending. | MSVC asserted in `two_awaits_in_expression`. Now each awaiter owns its child, pins it in await_suspend and links it into a small list (`awaited_child`) that `release_resources` walks. |
| 13 | **Cancelling an all_of/any_of mid-flight** (via `.into<cancelable_task>()`) leaked: children hold the frame weakly, the state held them strongly, and they were not even cancelled. | `cancel_combinator_midway`: 11 promises leaked and the "cancelled" counters ran all 1000 steps (15.6 s). The shared states now have `release_resources()` (removed in `177da91`, now safe): 0 leaks, 243 ms. |

## 3. Cleanup

- Removed debug leftovers: the handle registry (`g_active_handles`, `register_handle`), `__debugbreak`
  (not portable), `m_in_continuation_access`, `debug_print`, the self-include, `MUST_INLINE`,
  `NO_ASAN`, `deferred_guard`, `trivial_single_use_task`, `task_base_flag`, the `config_parent` hook,
  the duplicate `get_current_promise_t`, `resumed_promise_count`, and the uncallable
  `task(task_base<T>&&)` template constructor.
- `trivial_storage` + `memcpy` replaced by capturing the index (MSVC `move_only_function` stores 56 bytes
  inline, so there is no extra allocation).
- any_of no longer copies results into the shared state: the winner records its index and the parent
  reads the winner's result itself.
- Continuations are now `std::move_only_function<void()>` (nobody used the promise argument, and after
  the reordering in fix 1 it would not be safe to use).
- The four copies of the start-children logic are now one `start_children`.

## 4. Test results (Clang 22, -O2)

All 20 default tests pass. Promise heap allocations (whole test, all iterations):

| test | baseline | now |
|------|---------:|----:|
| chain / all_of_tuple / exceptions / block_on / co_spawn / from_future | same | same |
| all_of_vector, any_of_vector_fanout | crash | 120200 / 65000 |
| nested_mix (default `any_of`) | 341348 | 379983 (+11%, cost of loop safety) |
| nested_mix_once (`any_of_once`) | n/a | 340344 (no increase) |
| await_in_loop, any_of_in_loop, generator | crash/hang | pass |

## 4b. Inline resumption (same thread, same pool)

When a coroutine schedules another one with the same hint from a worker thread (a child finishing and
resuming its parent, a parent starting its child), the target goes into a thread_local `t_next` slot and
is resumed by the run loop right after the current coroutine, without going through the pool.

Medians of 5 alternating runs (Clang 22, -O2), whole test:

| test | before | after | speedup |
|------|-------:|------:|--------:|
| bench_sequential_awaits (20 x 20000 co_awaits) | 539 ms | 62 ms | x8.7 |
| bench_deep_chain (20 x depth 2000) | 74 ms | 17 ms | x4.4 |
| nested_mix | 445 ms | 187 ms | x2.4 |
| chain | 45 ms | 36 ms | x1.25 |
| all_of_vector | 169 ms | 140 ms | x1.2 |
| any_of_vector_fanout, all_of_tuple | | | x1.0 (dominated by creating thread pools per iteration) |

nested_mix also allocates fewer promises: the first child now often finishes before the others have
started, so cancelled losers never start their own subtrees.

Bug found while doing it: the pool job was `run_on_this_thread(std::exchange(handle, {}).lock())`. The
temporary weak handle lived until the call returned, keeping a finished `sleep_for` frame alive after
its parent had resumed; the parent built the next `sleep_for` in the same HALO slot and the late weak
release corrupted its counts (`await_in_loop`, only with HALO). Rule: release *all* child-side
references, weak ones included, before the parent can run.

## 4c. Second round of stress tests (fuzz + targeted)

New tests: `fuzz` (random coroutine trees mixing every feature, checked against a synchronous model;
`FUZZ_SEED=n` picks another batch), `deep_cancel`, `concurrent_block_on`, `generator_abandon`,
`destroy_with_pinned_loser`, `nested_co_spawn`, `interrupt_in_loop`. They found three more bugs:

| # | Bug | Fix |
|---|-----|-----|
| 14 | **HALO slot reuse through the owner chain.** `interrupt_by` (an elided `task`) awaits an `any_of`; the any_of frame is on the heap but held its owner weakly, and the still-running losers held the any_of frame. So the finished `interrupt_by` frame could not be destroyed while its slot in the caller's loop was already reused. Same for any elided coroutine that uses `any_of`. | `any_of_task` marks its promise "never elided", and such promises do not take an owner reference (`set_owner_if_needed`): they are not inside their owner, so the owner does not need to outlive them. |
| 15 | **Cancelling a deep chain overflowed the stack** (release_resources -> child release -> ..., and destroy -> owner release -> ...; a 20000-deep cancelable chain crashed, 5000 did not). | Releases are drained iteratively from a thread_local list by the outermost release on a thread. Only objects that hold their owner are queued; owner-less objects (never-started children, roots) are still handled immediately, because a never-started child can be elided into the very frame whose destruction releases it (a first, unconditional version crashed `nested_mix_once` exactly like that). |
| 16 | **co_spawn from inside a coroutine deadlocked during context destruction.** The destructor waited for `unique_lock` on a `shared_mutex` while the root called `co_spawn` -> `lock_shared()`, which Windows' SRW lock blocks behind the waiting writer. | The holder count is now a plain counter (mutex + condition_variable); `lock()` never blocks. |

## 4d. Structured any_of, timers, detach (the current design)

The HALO bugs in 4a-4c all had one root: an any_of loser (or something holding it) outlived the
co_await that started it, while its frame sat in a HALO slot that the caller reused. Instead of more
special cases (`any_of_once`, `any_of_task`, never-elided promises, non-elidable parameters, all now
removed), the library is now structured:

- `any_of` waits after the winner until every child (with its whole subtree) has been destroyed.
  Each wrapper gets an `on_destroyed` hook; `destroy()` runs it after destroying the frame and releasing
  the owner. A counter starting at children + 1 avoids lost wakeups (`wait_children_destroyed`).
  Everything can be elided again, in loops too.
- For that not to make any_of slow, waiting must not block threads and must be cancelable: the context
  has a timer (event loop) and `sleep_for` / `poll_until` / `from_future` / `wait_for_semaphore` are
  built on it (`delay` awaitable with a `cancel_hook` that removes the timer). A non-cancelable loser
  still runs to completion and delays any_of (kept on purpose); `detach(task)` opts out.
- Continuations never hold a strong reference to the coroutine they wake (`schedule_resume`), and the
  run loop drains `t_next` even when its own job had nothing to resume (fixed a lost wakeup that hung
  `any_of_vector_fanout`).
- Windows default timer resolution (~15.6 ms) rounds short delays up (`from_future` polls every 1 ms but
  takes ~15 ms). A high-resolution waitable timer in `timer_loop` would fix that if needed.

## 4e. Stale resumers: parents now wait for destruction

`any_of_in_loop` still failed ~2/100 after 4d. Instrumentation (a debug map of live promise addresses)
showed a new any_of promise being constructed on top of the previous iteration's still-alive any_of,
and, one level down, a finished `sleep_for` still holding strong=1 after its parent's await_resume.
Cause: thread I runs `sleep_for` into `co_await delay(...)`; the timer fires and thread J resumes and
finishes it and wakes the parent, which moves on and reuses the HALO slot, while I has not yet returned
from `resume()` and still holds its strong reference. No ordering on the child side can close that
window, so the rule became: a parent continues only once the child is destroyed (direct co_await via
the child's `on_destroyed` hook; all_of and any_of via `wait_children_destroyed`). Combinator children
use a shared `combinator_sync` block instead of referencing the combinator. With HALO disabled the old
code never failed (300 runs); with the fix: 0/150, and the overlap detector finds no overlap.

### Verification of the current state (structured design, 30 tests)

Clang 22 -O2, Clang 21 -O2 and MSVC /O2: all tests pass. ASan (Clang 22 -O1): every test individually,
2 passes, no errors. Fuzz: 20 extra seed batches (24000 random trees). 18 race-sensitive tests x 30
runs and `any_of_in_loop` x 150: 0 failures. A debug build that aborts when a promise is constructed on
top of a still-live one: 8 tests x 20 runs, 0 overlaps.

### Tried and rejected: a frame recycling allocator

A per-thread, size-class free-list allocator for non-elided frames (like Boost.Asio's recycling
allocator). Medians of 5 runs: Clang `bench_sequential_awaits` 157 -> 125 ms and `bench_deep_chain`
53 -> 36 ms, everything else within noise; MSVC (no HALO, every frame on the heap) unchanged. The Windows
heap is already fast for small blocks, and frames are often freed on another thread than the one that
allocated them, so the per-thread cache rarely hits. Not worth ~90 lines; removed.

### Accepted cost of the structured design

Waiting for the child's destruction instead of its completion makes the micro-benchmarks 2.5-3x slower
than right after inline resumption was added (`bench_sequential_awaits` 62 -> ~157 ms,
`bench_deep_chain` 18 -> ~53 ms), still ~3.5x faster than the original code. Considered acceptable;
optimize only if it shows up in real use.

## 5. Open issues / ideas

- `into_non_throw` of an `ex_task`/`any_of_task` creates a wrapper whose promise carries an unused
  shared state (harmless, wastes memory).
- Verified with Clang 22 (-O2, plus 3 full ASan runs), Clang 21 (-O2) and MSVC 14.44 (/O2): all 18
  default tests pass (20 after fixes 12 and 13). MSVC does no HALO, so its allocation counts are higher.
- `thread_pool::wait_for_tasks` uses `notify_one`; it is no longer used by the context, which waits
  on its own job counter.
