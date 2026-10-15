// Stress tests for coroutine.hpp.
//
// Usage: Coroutine [filter...]
//   With no arguments every test runs. Each argument is a substring filter; prefix it with '-' to
//   exclude matching tests instead (e.g. `Coroutine -generator`).
//
// Every iteration runs on a fresh multithreaded_execution_context (cycling through several thread
// counts) and, once that context has been destroyed, checks that no promise is still alive.

#define _CRT_SECURE_NO_WARNINGS // std::getenv

#include "coroutine.hpp"

#include <chrono>
#include <cstdint>
#include <cstdlib>
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

    // Structured any_of: returns only once the loser is gone; its slot is then reused by the awaits below.
    task<bool> test_any_of_then_more_awaits() {
        auto [fast, slow] = co_await coroutine::any_of(coroutine::sleep_for(0ms), coroutine::sleep_for(5ms));
        // One co_await per full expression: a promise awaits at most one child at a time (m_child_handle).
        int sum = co_await value_of(1);
        sum += co_await chain(20);
        co_await coroutine::sleep_for(1ms);
        sum += co_await value_of(2);
        co_return fast.has_value() != slow.has_value() && sum == 23;
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

    // Benchmarks: almost no work per coroutine, so the time is dominated by scheduling.
    task<bool> bench_sequential_awaits(int n) {
        long long sum = 0;
        for (int i = 0; i < n; ++i) {
            sum += co_await value_of(i);
        }
        co_return sum == static_cast<long long>(n) * (n - 1) / 2;
    }

    task<bool> bench_deep_chain() {
        co_return co_await chain(2000) == 2000;
    }

    std::generator<std::uint64_t> fibonacci(unsigned n);

    // ---------------------------------------------------------------------------------------------
    // Fuzz: a random tree of coroutines mixing every feature, checked against a synchronous model.
    // Values only depend on the seed (any_of / interrupt nodes ignore which child won), so the result
    // is deterministic even though scheduling is not.
    // ---------------------------------------------------------------------------------------------

    std::uint64_t mix(std::uint64_t x) {
        x += 0x9E3779B97F4A7C15ull;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
        return x ^ (x >> 31);
    }

    enum fuzz_kind {
        k_leaf, k_throw, k_sleep, k_await, k_all_tuple, k_all_vector, k_any_tuple, k_any_vector,
        k_loop, k_any_once, k_interrupt, k_cancel_all, k_kind_count
    };

    fuzz_kind kind_of(std::uint64_t seed, int depth) {
        if (depth == 0) {
            return static_cast<fuzz_kind>(mix(seed) % 3); // leaf, throw or sleep
        }
        return static_cast<fuzz_kind>(mix(seed) % k_kind_count);
    }

    long long leaf_value(std::uint64_t seed) {
        return static_cast<long long>(mix(seed ^ 0x1234) % 1000);
    }

    std::uint64_t child_seed(std::uint64_t seed, int i) {
        return mix(seed * 31 + static_cast<std::uint64_t>(i) + 1);
    }

    int child_count(std::uint64_t seed) {
        return 1 + static_cast<int>(mix(seed ^ 0x55) % 4);
    }

    long long value_or(const std::optional<long long> &v, long long fallback) {
        return v ? *v : fallback;
    }

    long long value_or(const std::expected<long long, std::exception_ptr> &v, long long fallback) {
        return v ? *v : fallback;
    }

    // The synchronous model. std::nullopt means "throws".
    std::optional<long long> fuzz_model(std::uint64_t seed, int depth) {
        switch (kind_of(seed, depth)) {
            case k_leaf: return leaf_value(seed);
            case k_throw: return std::nullopt;
            case k_sleep: return leaf_value(seed);
            case k_await: return value_or(fuzz_model(child_seed(seed, 0), depth - 1), 6) + 1;
            case k_all_tuple:
                return value_or(fuzz_model(child_seed(seed, 0), depth - 1), 13) +
                       value_or(fuzz_model(child_seed(seed, 1), depth - 1), 13);
            case k_all_vector: {
                long long sum = 0;
                for (int i = 0; i < child_count(seed); ++i) {
                    sum += value_or(fuzz_model(child_seed(seed, i), depth - 1), 13);
                }
                return sum;
            }
            case k_any_tuple: return 100 + leaf_value(seed);
            case k_any_vector: return 200 + leaf_value(seed);
            case k_loop: {
                long long sum = 0;
                for (int i = 0; i < 3; ++i) {
                    sum += value_or(fuzz_model(child_seed(seed, i), depth - 1), 7);
                }
                return sum;
            }
            case k_any_once: return 300 + leaf_value(seed);
            case k_interrupt: return 400 + leaf_value(seed);
            case k_cancel_all: return 500 + leaf_value(seed);
            default: return 0;
        }
    }

    template<template<typename> class TaskT>
    TaskT<long long> fuzz_node(std::uint64_t seed, int depth);

    // A child of either task type, chosen by the seed.
    task<long long> fuzz_child(std::uint64_t seed, int depth) {
        if (mix(seed ^ 0xC0FFEE) & 1) {
            co_return co_await fuzz_node<cancelable_task>(seed, depth);
        }
        co_return co_await fuzz_node<task>(seed, depth);
    }

    // any_of at the outermost scope of a coroutine body (it used to need any_of_once).
    task<long long> fuzz_any_once(std::uint64_t seed, int depth) {
        co_await coroutine::any_of(fuzz_node<task>(child_seed(seed, 0), depth - 1),
                                        fuzz_node<cancelable_task>(child_seed(seed, 1), depth - 1));
        co_return 300 + leaf_value(seed);
    }

    template<template<typename> class TaskT>
    TaskT<long long> fuzz_node(std::uint64_t seed, int depth) {
        switch (kind_of(seed, depth)) {
            case k_leaf:
                co_return leaf_value(seed);
            case k_throw:
                throw std::runtime_error("fuzz");
            case k_sleep:
                co_await coroutine::sleep_for(std::chrono::milliseconds(mix(seed) % 2));
                co_return leaf_value(seed);
            case k_await: {
                long long v = 6;
                try {
                    v = co_await fuzz_child(child_seed(seed, 0), depth - 1);
                } catch (const std::runtime_error &) {
                }
                co_return v + 1;
            }
            case k_all_tuple: {
                auto [a, b] = co_await coroutine::all_of(fuzz_node<task>(child_seed(seed, 0), depth - 1),
                                                         fuzz_node<cancelable_task>(child_seed(seed, 1), depth - 1));
                co_return value_or(a, 13) + value_or(b, 13);
            }
            case k_all_vector: {
                std::vector<task<long long>> children;
                for (int i = 0; i < child_count(seed); ++i) {
                    children.push_back(fuzz_child(child_seed(seed, i), depth - 1));
                }
                long long sum = 0;
                for (auto &r: co_await coroutine::all_of(std::move(children))) {
                    sum += value_or(r, 13);
                }
                co_return sum;
            }
            case k_any_tuple:
                co_await coroutine::any_of(fuzz_node<cancelable_task>(child_seed(seed, 0), depth - 1),
                                           fuzz_node<task>(child_seed(seed, 1), depth - 1));
                co_return 100 + leaf_value(seed);
            case k_any_vector: {
                std::vector<cancelable_task<long long>> children;
                for (int i = 0; i < child_count(seed); ++i) {
                    children.push_back(fuzz_node<cancelable_task>(child_seed(seed, i), depth - 1));
                }
                co_await coroutine::any_of(std::move(children));
                co_return 200 + leaf_value(seed);
            }
            case k_loop: {
                long long sum = 0;
                for (int i = 0; i < 3; ++i) {
                    try {
                        sum += co_await fuzz_child(child_seed(seed, i), depth - 1);
                    } catch (const std::runtime_error &) {
                        sum += 7;
                    }
                }
                co_return sum;
            }
            case k_any_once:
                co_return co_await fuzz_any_once(seed, depth);
            case k_interrupt:
                try {
                    co_await fuzz_node<task>(child_seed(seed, 0), depth - 1)
                            .template into<coroutine::interruptable_task>()
                            .interrupt_by(coroutine::sleep_for(std::chrono::milliseconds(mix(seed) % 2)));
                } catch (...) {
                }
                co_return 400 + leaf_value(seed);
            case k_cancel_all:
                // The all_of usually loses and is cancelled while its children still run.
                co_await coroutine::any_of(
                    fuzz_node<task>(child_seed(seed, 0), 0),
                    coroutine::all_of(fuzz_node<cancelable_task>(child_seed(seed, 1), depth - 1),
                                      fuzz_node<cancelable_task>(child_seed(seed, 2), depth - 1))
                    .template into<cancelable_task>());
                co_return 500 + leaf_value(seed);
            default:
                co_return 0;
        }
    }

    task<bool> test_fuzz(std::uint64_t seed, int depth) {
        const auto expected = fuzz_model(seed, depth);
        try {
            const long long got = co_await fuzz_node<task>(seed, depth);
            co_return expected && got == *expected;
        } catch (const std::runtime_error &) {
            co_return !expected;
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Targeted stress tests
    // ---------------------------------------------------------------------------------------------

    // A deep chain of cancelable tasks, cancelled while its bottom is still waiting.
    cancelable_task<int> cancelable_chain(int depth) {
        if (depth == 0) {
            co_await coroutine::sleep_for(50ms);
            co_return 0;
        }
        co_return 1 + co_await cancelable_chain(depth - 1);
    }

    task<bool> test_deep_cancel(int depth) {
        auto [fast, deep] = co_await coroutine::any_of(delayed_value(10ms, 1), cancelable_chain(depth));
        co_return fast.has_value() && !deep.has_value();
    }

    task<bool> test_generator_abandon() {
        {
            auto gen = coroutine::asyncify(fibonacci(50));
            for (int i = 0; i < 3; ++i) {
                if (!co_await gen) {
                    co_return false;
                }
            }
        } // dropped while suspended at a co_yield
        auto a = coroutine::asyncify(fibonacci(40));
        auto b = coroutine::asyncify(fibonacci(40));
        std::uint64_t last = 0;
        for (int i = 0; i < 40; ++i) {
            auto va = co_await a;
            auto vb = co_await b;
            if (!va || !vb || *va != *vb) {
                co_return false;
            }
            last = *va;
        }
        const bool a_done = !(co_await a);
        const bool b_done = !(co_await b);
        co_return a_done && b_done && last == 63245986;
    }

    // The root finishes while a pinned loser (sleep_for 30 ms) is still running; the context's
    // destructor must wait for it.
    task<void> root_with_pinned_loser(std::atomic<int> *done) {
        co_await coroutine::any_of(coroutine::sleep_for(0ms), coroutine::sleep_for(30ms));
        done->fetch_add(1);
    }

    // Spawns more roots from inside a coroutine while the owner may already be destroying the context.
    task<void> spawn_more(std::atomic<int> *done, int depth) {
        if (depth > 0) {
            auto *ctx = co_await coroutine::get_current_coroutine_context();
            ctx->co_spawn(spawn_more(done, depth - 1));
            ctx->co_spawn(spawn_more(done, depth - 1));
        }
        co_await coroutine::sleep_for(0ms);
        done->fetch_add(1);
    }

    task<bool> test_interrupt_in_loop() {
        int completed = 0, interrupted = 0;
        for (int i = 0; i < 30; ++i) {
            try {
                co_await coroutine::sleep_for(std::chrono::milliseconds(i % 2))
                        .into<coroutine::interruptable_task>()
                        .interrupt_by(coroutine::sleep_for(std::chrono::milliseconds((i + 1) % 2)));
                ++completed;
            } catch (const coroutine::interrupted_exception &) {
                ++interrupted;
            }
        }
        co_return completed + interrupted == 30;
    }

    // Really blocks a worker thread (cannot be interrupted).
    task<int> blocking_work(std::atomic<int> *done, int ms) {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        done->fetch_add(1);
        co_return 42;
    }

    task<bool> test_detach(std::atomic<int> *done) {
        const int v = co_await coroutine::detach(value_of(5));
        bool threw = false;
        try {
            co_await coroutine::detach(throw_if_negative(-1));
        } catch (const std::runtime_error &) {
            threw = true;
        }
        // The detached loser must not hold up any_of, even though it blocks a thread for 200 ms.
        const auto start = std::chrono::steady_clock::now();
        auto [fast, slow] = co_await coroutine::any_of(coroutine::sleep_for(1ms),
                                                       coroutine::detach(blocking_work(done, 200)));
        const auto elapsed = std::chrono::steady_clock::now() - start;
        co_return v == 5 && threw && fast.has_value() && elapsed < 150ms;
    }

    // A non-cancelable loser that has started runs to completion, and any_of returns only after it has.
    // (One that has not started yet when the winner finishes is simply dropped: a task pins itself only
    // once it runs. Hence the delayed winner.)
    task<int> pinned_loser(std::atomic<int> *finished) {
        co_await coroutine::sleep_for(20ms);
        finished->store(1);
        co_return 2;
    }

    task<bool> test_any_of_waits_for_pinned_loser() {
        std::atomic<int> finished{0};
        auto [fast, slow] = co_await coroutine::any_of(delayed_value(5ms, 1), pinned_loser(&finished));
        co_return fast.has_value() && **fast == 1 && !slow.has_value() && finished.load() == 1;
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
    run("any_of_then_more_awaits", 30, [](auto &ctx) { CHECK(ctx.block_on(test_any_of_then_more_awaits())); });
    run("any_of_waits_for_pinned_loser", 30, [](auto &ctx) {
        CHECK(ctx.block_on(test_any_of_waits_for_pinned_loser()));
    });
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
    run("fuzz", 300, [](auto &ctx) {
        // FUZZ_SEED picks a different batch of trees; schedules differ from run to run anyway.
        static std::uint64_t next_seed = [] {
            const char *env = std::getenv("FUZZ_SEED");
            return env ? std::strtoull(env, nullptr, 10) * 100000 + 1 : 1;
        }();
        for (int i = 0; i < 4; ++i) {
            const std::uint64_t seed = next_seed++;
            if (!ctx.block_on(test_fuzz(seed, 5))) {
                std::println(stderr, "    fuzz seed {} gave a wrong result", seed);
                ++g_failures;
            }
        }
        ctx.wait_idle(); // pinned losers may still be running
    });
    run("deep_cancel", 5, [](auto &ctx) { CHECK(ctx.block_on(test_deep_cancel(20000))); });
    run("concurrent_block_on", 10, [](auto &ctx) {
        std::atomic<int> ok{0};
        {
            std::vector<std::jthread> threads;
            for (int t = 0; t < 8; ++t) {
                threads.emplace_back([&] {
                    for (int i = 0; i < 50; ++i) {
                        ok += ctx.block_on(test_all_of_tuple()) ? 1 : 0;
                    }
                });
            }
        }
        CHECK(ok.load() == 8 * 50);
    });
    run("generator_abandon", 100, [](auto &ctx) { CHECK(ctx.block_on(test_generator_abandon())); });
    run("destroy_with_pinned_loser", 20, [](auto &) {
        std::atomic<int> done{0};
        {
            coroutine::multithreaded_execution_context local{2, 2};
            local.co_spawn(root_with_pinned_loser(&done));
        }
        CHECK(done.load() == 1);
    });
    run("nested_co_spawn", 20, [](auto &) {
        std::atomic<int> done{0};
        {
            coroutine::multithreaded_execution_context local{4, 4};
            local.co_spawn(spawn_more(&done, 6));
        }
        CHECK(done.load() == 127); // 2^7 - 1 coroutines
    });
    run("interrupt_in_loop", 10, [](auto &ctx) { CHECK(ctx.block_on(test_interrupt_in_loop())); });
    run("detach", 10, [](auto &) {
        std::atomic<int> done{0};
        {
            coroutine::multithreaded_execution_context local{2, 2};
            CHECK(local.block_on(test_detach(&done)));
        } // the destructor waits for the detached task
        CHECK(done.load() == 1);
    });
    run("bench_sequential_awaits", 20, [](auto &ctx) { CHECK(ctx.block_on(bench_sequential_awaits(20000))); });
    run("bench_deep_chain", 20, [](auto &ctx) { CHECK(ctx.block_on(bench_deep_chain())); });
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
