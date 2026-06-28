#include "coroutine.hpp"

using coroutine::task;
using coroutine::cancelable_task;

task<void> say1() {
    // std::this_thread::sleep_for(std::chrono::milliseconds(10));
    // std::cout << 1;
    co_return;
}

// volatile char *volatile something = nullptr;
//
// task<void> say2() {
//     volatile char padding[64];
//     padding[0] = 1;
//     something = padding;
//     std::cout << 2;
//     co_await say1();
//     co_return;
// }
//
// cancelable_task<void> say3() {
//     auto *context = co_await coroutine::get_current_coroutine_context();
//     co_await say2().with_context(context);
// }


cancelable_task<void> do_many_things(int n) {
    if (n < 2) {
        co_return;
    }

    co_await coroutine::any_of(do_many_things(n - 3), do_many_things(n - 2),
                               coroutine::any_of(do_many_things(n - 4), do_many_things(n - 3)));

    // co_await do_many_things(n - 1), co_await do_many_things(n - 2);

    co_await coroutine::all_of(do_many_things(n - 3), do_many_things(n - 4));


    std::cout << n;
}

task<void> test_interrupt() {
    co_await coroutine::sleep_for(std::chrono::milliseconds(1))
            .into<coroutine::interruptable_task>()
            .interrupt_by(coroutine::sleep_for(std::chrono::milliseconds(1)));
}

std::generator<uint64_t> fibonacci_sequence(unsigned n) {
    if (n == 0)
        co_return;

    if (n > 94)
        throw std::runtime_error("Too big Fibonacci sequence. Elements would overflow.");

    co_yield 0;

    if (n == 1)
        co_return;

    co_yield 1;

    if (n == 2)
        co_return;

    std::uint64_t a = 0;
    std::uint64_t b = 1;

    for (unsigned i = 2; i < n; ++i) {
        std::uint64_t s = a + b;
        co_yield s;
        a = b;
        b = s;
    }
}

task<void> test_fibonacci() {
    auto fib_gen = coroutine::asyncify(fibonacci_sequence(10));

    while (auto res = co_await fib_gen) {
        std::cout << *res << " ";
    }
    std::cout << std::endl;

    co_return;
}

task<void> test_any_simple() {
    auto [task1, task2] = co_await coroutine::any_of(
        coroutine::sleep_for(std::chrono::milliseconds(10)),
        coroutine::sleep_for(std::chrono::milliseconds(20)));

    if (task1) {
        std::cout << "Task 1 completed first." << std::endl;
    } else if (task2) {
        std::cout << "Task 2 completed first." << std::endl;
    } else {
        std::cout << "No task completed." << std::endl;
    }
}

cancelable_task<void> test_any_of() {
    co_await coroutine::sleep_for(std::chrono::milliseconds(100));

    co_await coroutine::any_of(
        coroutine::sleep_for(std::chrono::milliseconds(10)),
        coroutine::any_of(coroutine::sleep_for(std::chrono::milliseconds(20)),
        coroutine::any_of(coroutine::sleep_for(std::chrono::milliseconds(30)), coroutine::sleep_for(std::chrono::milliseconds(30)),
        coroutine::any_of(coroutine::sleep_for(std::chrono::milliseconds(30)), coroutine::sleep_for(std::chrono::milliseconds(30)),
        coroutine::any_of(coroutine::sleep_for(std::chrono::milliseconds(30)), coroutine::sleep_for(std::chrono::milliseconds(30)),
        coroutine::any_of(coroutine::sleep_for(std::chrono::milliseconds(30))))))));
}

cancelable_task<void> test_cancel_task() {
    co_await coroutine::sleep_for(std::chrono::milliseconds(1));
    co_await coroutine::sleep_for(std::chrono::milliseconds(1));
    co_await coroutine::sleep_for(std::chrono::milliseconds(1));
    co_await coroutine::sleep_for(std::chrono::milliseconds(1));
    co_await coroutine::sleep_for(std::chrono::milliseconds(1));
    co_await coroutine::sleep_for(std::chrono::milliseconds(1));
    co_await coroutine::sleep_for(std::chrono::milliseconds(1));
    co_await coroutine::sleep_for(std::chrono::milliseconds(1));
    co_await coroutine::sleep_for(std::chrono::milliseconds(1));

    auto promise = co_await coroutine::_details::get_current_promise_base();

    std::cout << "This should not be printed" << std::endl;
}

NO_ASAN int main() {
    for (int i = 0; i < 100; ++i) {
        {
            std::cout << std::format("\nNow running test_many_asm() iteration {}\n", i) << std::endl;

            auto execution_ctx = coroutine::_details::multithreaded_execution_context{4};

            try {
                execution_ctx.async_execute(coroutine::any_of(test_cancel_task(), say1())).get();
            } catch (const std::exception &e) {
                std::cout << "Caught exception: " << e.what() << std::endl;
            }
        }
        // std::cout << std::endl;


        std::cout << std::endl;
        std::cout << coroutine::_details::debug_get_active_promise_count();

        std::cout << std::endl;
        std::cout << coroutine::execution_context::resumed_promise_count.load(std::memory_order_acquire) << std::endl;
    }

    std::cout << "Heap allocations: " << std::endl;

    std::cout << coroutine::_details::promise_base::g_heap_alloc_count.load(std::memory_order_acquire) << std::endl;

    std::cout << "Active promises: " << std::endl;

    std::cout << coroutine::_details::debug_get_active_promise_count() << std::endl;
}
