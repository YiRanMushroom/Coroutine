// Stress tests for coroutine.hpp.
//
// Usage: Coroutine [filter...]
//   With no arguments every test runs. Each argument is a substring filter; prefix it with '-' to
//   exclude matching tests instead (e.g. `Coroutine -generator`).
//
// Every iteration runs on a fresh multithreaded_execution_context (cycling through several thread
// counts) and, once that context has been destroyed, checks that no promise is still alive.

#include "coroutine.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <generator>
#include <print>
#include <string_view>
#include <vector>

using namespace std::chrono_literals;
using coroutine::cancelable_task;
using coroutine::task;

namespace {
    int g_failures = 0;

#define CHECK(...)                                                                                  \
    do {                                                                                            \
        if (!(__VA_ARGS__)) {                                                                       \
            std::println(stderr, "    CHECK failed: {}  ({}:{})", #__VA_ARGS__, __FILE__, __LINE__); \
            ++g_failures;                                                                           \
        }                                                                                           \
    } while (0)

    // ---------------------------------------------------------------------------------------------
    // Building blocks
    // ---------------------------------------------------------------------------------------------

    task<int> value_of(int v) {
        co_return v;
    }

    task<int> throw_if_negative(int v) {
        if (v < 0) {
            throw std::runtime_error("negative");
        }
        co_return v;
    }

    // Sequential chain: every level is its own coroutine, resumed through the thread pool.
    task<int> chain(int depth) {
        if (depth == 0) {
            co_return 0;
        }
        co_return 1 + co_await chain(depth - 1);
    }

    task<int> delayed_value(std::chrono::milliseconds delay, int v) {
        co_await coroutine::sleep_for(delay);
        co_return v;
    }

    // Counts its steps so the test can tell whether it was stopped early.
    cancelable_task<int> slow_counter(std::atomic<int> *progress, int steps) {
        for (int i = 0; i < steps; ++i) {
            co_await coroutine::sleep_for(1ms);
            progress->fetch_add(1);
        }
        co_return -1;
    }

    long long expected_vector_sum(int n) {
        long long sum = 0;
        for (int i = 0; i < n; ++i) {
            sum += (i % 3 == 0) ? i % 7 : i;
        }
        return sum;
    }

    std::vector<task<int>> make_mixed_tasks(int n) {
        std::vector<task<int>> tasks;
        tasks.reserve(n);
        for (int i = 0; i < n; ++i) {
            tasks.push_back((i % 3 == 0) ? chain(i % 7) : value_of(i));
        }
        return tasks;
    }

    // ---------------------------------------------------------------------------------------------
    // Test bodies (each returns true on success)
    // ---------------------------------------------------------------------------------------------

    task<bool> test_chain() {
        co_return co_await chain(300) == 300;
    }

    task<bool> test_all_of_tuple() {
        auto [a, b, c] = co_await coroutine::all_of(value_of(1), value_of(2), chain(10));
        co_return a.has_value() && b.has_value() && c.has_value() && *a + *b + *c == 13;
    }

    task<bool> test_all_of_vector() {
        constexpr int n = 200;
        auto results = co_await coroutine::all_of(make_mixed_tasks(n));
        long long sum = 0;
        for (auto &r: results) {
            if (!r) {
                co_return false;
            }
            sum += *r;
        }
        co_return results.size() == n && sum == expected_vector_sum(n);
    }

    // Many children that finish immediately: the first one can resume the parent while the
    // remaining children are still being started.
    task<bool> test_any_of_vector_fanout() {
        constexpr int n = 64;
        std::vector<task<int>> tasks;
        for (int i = 0; i < n; ++i) {
            tasks.push_back(value_of(i));
        }
        auto results = co_await coroutine::any_of(std::move(tasks));
        int engaged = 0;
        bool values_ok = true;
        for (std::size_t i = 0; i < results.size(); ++i) {
            if (results[i]) {
                ++engaged;
                values_ok = values_ok && results[i]->has_value() && **results[i] == static_cast<int>(i);
            }
        }
        co_return results.size() == n && engaged == 1 && values_ok;
    }

    task<bool> test_any_of_tuple_fanout() {
        auto [a, b, c, d] = co_await coroutine::any_of(value_of(0), value_of(1), chain(5), value_of(3));
        const int engaged = a.has_value() + b.has_value() + c.has_value() + d.has_value();
        co_return engaged == 1;
    }

    // The loser of any_of is a cancelable_task: it must stop long before finishing its 1000 steps.
    task<bool> test_any_of_cancels_loser(std::atomic<int> *progress) {
        auto [fast, slow] = co_await coroutine::any_of(delayed_value(20ms, 7), slow_counter(progress, 1000));
        co_return fast.has_value() && fast->has_value() && **fast == 7 && !slow.has_value();
    }

    // co_await in a loop: with HALO every iteration's child frame lives in the same slot of our frame.
    task<bool> test_await_in_loop() {
        int sum = 0;
        for (int i = 0; i < 200; ++i) {
            sum += co_await value_of(i);
            co_await coroutine::sleep_for(0ms);
        }
        co_return sum == 199 * 200 / 2;
    }

    // any_of in a loop whose loser is a pinned (non-cancelable) io_task that is still running when
    // the next iteration starts.
    task<bool> test_any_of_in_loop() {
        int first_won = 0;
        for (int i = 0; i < 20; ++i) {
            auto [fast, slow] = co_await coroutine::any_of(coroutine::sleep_for(0ms), coroutine::sleep_for(3ms));
            first_won += fast.has_value();
        }
        co_return first_won > 0;
    }

    // any_of_once at the outermost scope: the pinned loser (5 ms) is still running while we go on.
    task<bool> test_any_of_once_top_level() {
        auto [fast, slow] = co_await coroutine::any_of_once(coroutine::sleep_for(0ms), coroutine::sleep_for(5ms));
        // One co_await per full expression: a promise awaits at most one child at a time (m_child_handle).
        int sum = co_await value_of(1);
        sum += co_await chain(20);
        co_await coroutine::sleep_for(1ms);
        sum += co_await value_of(2);
        co_return fast.has_value() != slow.has_value() && sum == 23;
    }

    // NOT safe, kept to demonstrate why any_of_once must not be used in a loop (opt-in test).
    task<bool> test_any_of_once_in_loop() {
        for (int i = 0; i < 20; ++i) {
            co_await coroutine::any_of_once(coroutine::sleep_for(0ms), coroutine::sleep_for(3ms));
        }
        co_return true;
    }

    // Two co_awaits in one expression. The operands of + are unsequenced, so a compiler may run both
    // await_transforms before suspending for either (MSVC does).
    task<bool> test_two_awaits_in_expression() {
        const int sum = co_await value_of(1) + co_await value_of(2);
        bool threw = false;
        try {
            // If the left child throws, the right one may already have been transformed but never
            // started; it must still be released (checked by the leak check).
            [[maybe_unused]] int unused = co_await throw_if_negative(-1) + co_await value_of(2);
        } catch (const std::runtime_error &) {
            threw = true;
        }
        co_return sum == 3 && threw;
    }

    // all_of / any_of converted to cancelable tasks and cancelled while their children still run: the
    // children must be cancelled too, and everything destroyed (checked by the leak check).
    task<bool> test_cancel_combinator_midway(std::atomic<int> *progress) {
        auto [fast1, slow1] = co_await coroutine::any_of(
            delayed_value(10ms, 1),
            coroutine::all_of(slow_counter(progress, 1000), slow_counter(progress, 1000))
            .into<coroutine::cancelable_task>());
        auto [fast2, slow2] = co_await coroutine::any_of(
            delayed_value(10ms, 2),
            coroutine::any_of(slow_counter(progress, 1000), slow_counter(progress, 1000))
            .into<coroutine::cancelable_task>());
        co_return fast1.has_value() && !slow1.has_value() && fast2.has_value() && !slow2.has_value();
    }

    task<bool> test_exceptions() {
        auto [ok, bad] = co_await coroutine::all_of(value_of(1), throw_if_negative(-1));
        bool result = ok.has_value() && *ok == 1 && !bad.has_value();

        try {
            co_await throw_if_negative(-1);
            result = false;
        } catch (const std::runtime_error &) {
        }

        auto [only] = co_await coroutine::any_of(throw_if_negative(-2));
        result = result && only.has_value() && !only->has_value();
        co_return result;
    }

    // The original recursive test: nested any_of / all_of, exceptions at the leaves.
    cancelable_task<void> do_many_things(int n, std::atomic<int> *completed) {
        if (n < 2) {
            throw std::runtime_error("leaf");
        }

        co_await coroutine::any_of(do_many_things(n - 3, completed), do_many_things(n - 2, completed),
                                   coroutine::any_of(do_many_things(n - 4, completed),
                                                     do_many_things(n - 3, completed)));
        co_await coroutine::all_of(do_many_things(n - 3, completed), do_many_things(n - 4, completed));
        completed->fetch_add(1);
    }

    task<bool> test_nested_mix(std::atomic<int> *completed) {
        co_await do_many_things(14, completed);
        co_return completed->load() > 0;
    }

    // Same tree with any_of_once (used at the outermost scope of the coroutine body, so it is safe).
    cancelable_task<void> do_many_things_once(int n, std::atomic<int> *completed) {
        if (n < 2) {
            throw std::runtime_error("leaf");
        }

        co_await coroutine::any_of_once(do_many_things_once(n - 3, completed), do_many_things_once(n - 2, completed),
                                        coroutine::any_of_once(do_many_things_once(n - 4, completed),
                                                               do_many_things_once(n - 3, completed)));
        co_await coroutine::all_of(do_many_things_once(n - 3, completed), do_many_things_once(n - 4, completed));
        completed->fetch_add(1);
    }

    task<bool> test_nested_mix_once(std::atomic<int> *completed) {
        co_await do_many_things_once(14, completed);
        co_return completed->load() > 0;
    }

    std::generator<std::uint64_t> fibonacci(unsigned n) {
        std::uint64_t a = 0, b = 1;
        for (unsigned i = 0; i < n; ++i) {
            co_yield a;
            b = std::exchange(a, b) + b;
        }
    }

    task<bool> test_generator() {
        auto gen = coroutine::asyncify(fibonacci(30));
        std::vector<std::uint64_t> got;
        while (auto value = co_await gen) {
            got.push_back(*value);
        }

        std::vector<std::uint64_t> expected;
        for (auto v: fibonacci(30)) {
            expected.push_back(v);
        }
        co_return got == expected;
    }

    task<bool> test_interrupt() {
        bool interrupted = false;
        try {
            co_await coroutine::sleep_for(100ms)
                    .into<coroutine::interruptable_task>()
                    .interrupt_by(coroutine::sleep_for(1ms));
        } catch (const coroutine::interrupted_exception &) {
            interrupted = true;
        }

        bool completed = true;
        try {
            co_await coroutine::sleep_for(1ms)
                    .into<coroutine::interruptable_task>()
                    .interrupt_by(coroutine::sleep_for(100ms));
        } catch (...) {
            completed = false;
        }
        co_return interrupted && completed;
    }

    task<bool> test_from_future() {
        const int v = co_await coroutine::from_future(std::async(std::launch::async, [] { return 5; }));
        co_return v == 5;
    }

    task<void> spawned_root(std::atomic<int> *sum) {
        auto [a, b] = co_await coroutine::all_of(value_of(1), chain(2));
        sum->fetch_add(*a + *b);
    }

    // The test from the previous main(): any_of between a cancelable task and a plain one.
    cancelable_task<void> five_sleeps() {
        for (int i = 0; i < 5; ++i) {
            co_await coroutine::sleep_for(1ms);
        }
    }

    task<void> nothing() {
        co_return;
    }

    cancelable_task<void> test_cancel_task(std::atomic<int> *done) {
        co_await coroutine::any_of(five_sleeps(), nothing());
        co_await coroutine::sleep_for(1ms);
        done->fetch_add(1);
    }

    // ---------------------------------------------------------------------------------------------
    // Driver
    // ---------------------------------------------------------------------------------------------

    std::vector<std::string_view> g_filters;

    // Tests named "unsafe_..." only run when a filter explicitly selects them.
    bool selected(std::string_view name) {
        bool has_include = false, included = false;
        for (auto f: g_filters) {
            if (f.starts_with('-')) {
                if (name.contains(f.substr(1))) {
                    return false;
                }
            } else {
                has_include = true;
                included = included || name.contains(f);
            }
        }
        return name.starts_with("unsafe_") ? included : !has_include || included;
    }

    // Runs `body(ctx)` `iterations` times, each on a fresh context; checks for leaked promises.
    template<typename Body>
    void run(std::string_view name, int iterations, Body body) {
        if (!selected(name)) {
            return;
        }
        static constexpr uint32_t thread_counts[] = {1, 2, 4, 8};

        const int failures_before = g_failures;
        const auto allocs_before = coroutine::_details::promise_base::g_heap_alloc_count.load();
        const auto start = std::chrono::steady_clock::now();

        for (int i = 0; i < iterations; ++i) {
            {
                coroutine::multithreaded_execution_context ctx{thread_counts[i % 4], 4};
                body(ctx);
            }
            const auto alive = coroutine::_details::debug_get_active_promise_count();
            if (alive != 0) {
                std::println(stderr, "    iteration {}: {} promise(s) still alive", i, alive);
                ++g_failures;
                break;
            }
        }

        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        const auto allocs = coroutine::_details::promise_base::g_heap_alloc_count.load() - allocs_before;
        std::println("[{}] {:<26} {:>4} iterations  {:>6} ms  {:>7} promise heap allocs",
                     g_failures == failures_before ? " OK " : "FAIL", name, iterations, ms, allocs);
    }
}

int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0); // keep the output if a test crashes
    for (int i = 1; i < argc; ++i) {
        g_filters.emplace_back(argv[i]);
    }

    run("chain", 50, [](auto &ctx) { CHECK(ctx.block_on(test_chain())); });
    run("all_of_tuple", 500, [](auto &ctx) { CHECK(ctx.block_on(test_all_of_tuple())); });
    run("all_of_vector", 200, [](auto &ctx) { CHECK(ctx.block_on(test_all_of_vector())); });
    run("any_of_vector_fanout", 500, [](auto &ctx) { CHECK(ctx.block_on(test_any_of_vector_fanout())); });
    run("any_of_tuple_fanout", 500, [](auto &ctx) { CHECK(ctx.block_on(test_any_of_tuple_fanout())); });
    run("await_in_loop", 50, [](auto &ctx) { CHECK(ctx.block_on(test_await_in_loop())); });
    run("any_of_in_loop", 30, [](auto &ctx) { CHECK(ctx.block_on(test_any_of_in_loop())); });
    run("two_awaits_in_expression", 200, [](auto &ctx) { CHECK(ctx.block_on(test_two_awaits_in_expression())); });
    run("cancel_combinator_midway", 5, [](auto &ctx) {
        std::atomic<int> progress{0};
        CHECK(ctx.block_on(test_cancel_combinator_midway(&progress)));
        ctx.wait_idle();
        CHECK(progress.load() < 400); // 4 counters x 1000 steps if nothing were cancelled
    });
    run("exceptions", 300,[](auto &ctx) { CHECK(ctx.block_on(test_exceptions())); });
    run("any_of_cancels_loser", 10, [](auto &ctx) {
        std::atomic<int> progress{0};
        CHECK(ctx.block_on(test_any_of_cancels_loser(&progress)));
        ctx.wait_idle();
        CHECK(progress.load() < 200); // would reach 1000 if the loser were not cancelled
    });
    run("nested_mix", 30, [](auto &ctx) {
        std::atomic<int> completed{0};
        CHECK(ctx.block_on(test_nested_mix(&completed)));
        // Pinned (non-cancelable) losers keep running after block_on returns and still use `completed`.
        ctx.wait_idle();
    });
    run("nested_mix_once", 30, [](auto &ctx) {
        std::atomic<int> completed{0};
        CHECK(ctx.block_on(test_nested_mix_once(&completed)));
        ctx.wait_idle();
    });
    run("any_of_once_top_level", 30, [](auto &ctx) { CHECK(ctx.block_on(test_any_of_once_top_level())); });
    run("unsafe_any_of_once_in_loop", 30, [](auto &ctx) { CHECK(ctx.block_on(test_any_of_once_in_loop())); });
    run("generator", 100, [](auto &ctx) { CHECK(ctx.block_on(test_generator())); });
    run("interrupt", 5, [](auto &ctx) { CHECK(ctx.block_on(test_interrupt())); });
    run("from_future", 100, [](auto &ctx) { CHECK(ctx.block_on(test_from_future())); });
    run("block_on_repeated", 5, [](auto &ctx) {
        for (int i = 0; i < 200; ++i) {
            CHECK(ctx.block_on(test_all_of_tuple()));
        }
    });
    run("co_spawn_many", 20, [](auto &ctx) {
        std::atomic<int> sum{0};
        {
            // The context is destroyed at the end of run()'s iteration scope, which waits for every
            // spawned root; use a nested context here so the check below sees the final sum.
            coroutine::multithreaded_execution_context local{4, 4};
            for (int i = 0; i < 300; ++i) {
                local.co_spawn(spawned_root(&sum));
            }
        }
        CHECK(sum.load() == 300 * 3);
    });
    run("cancel_task(original)", 50, [](auto &ctx) {
        std::atomic<int> done{0};
        {
            coroutine::multithreaded_execution_context local{};
            local.co_spawn(test_cancel_task(&done));
        }
        CHECK(done.load() == 1);
    });

    std::println("\n{} check(s) failed. Promise heap allocations in total: {}", g_failures,
                 coroutine::_details::promise_base::g_heap_alloc_count.load());
    return g_failures == 0 ? 0 : 1;
}
