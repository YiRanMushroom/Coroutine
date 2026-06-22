#include "coroutine.hpp"

using coroutine::task;
using coroutine::cancelable_task;

cancelable_task<void> say1() {
    std::cout << 1 << std::endl;
    co_return;
}

task<void> say2() {
    std::cout << 2 << std::endl;
    co_await say1();
    co_return;
}

cancelable_task<void> say3() {
    std::cout << 3 << std::endl;
    co_await say2();
    std::cout << "This should be cancelled";
    std::this_thread::sleep_for(std::chrono::seconds(5));
    co_await say1();
    std::cout << "This should not be printed";
    co_return;
}

// cancelable_task<void> say_but_slow() {
//     std::cout << "Starting slow task..." << std::endl;
//     co_await coroutine::sleep_for(std::chrono::seconds(2));
//     std::cout << "Finished slow task!" << std::endl;
//     co_await say1();
//     co_return;
// }

// cancelable_task<void> test_and() {
//     co_await coroutine::all_of(say_but_slow(), say_but_slow(), say_but_slow());
//     co_return;
// }
//
// task<void> test_or() {
//     co_await coroutine::any_of<task>(say_but_slow().into(), say_but_slow().into(), say1());
// }

coroutine::generator<int> count_up_to(int n) {
    for (int i = 0; i < n; ++i) {
        co_yield i;
    }
}

task<void> say_to_n(int n) {
    coroutine::generator<int> gen = count_up_to(n);

    std::cout << std::format("Handle {}", (void *) gen.get_promise()) << std::endl;

    std::cout << "Counting up to " << n << ":" << std::endl;

    while (auto result = co_await gen) {
        auto unwrapped = std::move(result).value();
        std::cout << std::format("Got value: {0}, handle: {1}\n", unwrapped, (void *) gen.get_promise()) << std::flush;
    }
    std::cout << "Done counting!" << std::endl;
    co_return;
}

cancelable_task<void> do_many_things(int n) {
    if (n < 2) {
        co_return;
    }

    co_await do_many_things(n - 1), co_await do_many_things(n - 2);

    co_await coroutine::all_of(do_many_things(n - 3), do_many_things(n - 4));

    co_await coroutine::any_of(do_many_things(n - 2), do_many_things(n - 3));

    std::cout << n;
}

int main() {
    for (int i = 0; i < 200; ++i) {
        {
            auto execution_ctx = coroutine::_details::multithreaded_execution_context{};

            // auto t = say3();

            execution_ctx.block_on(do_many_things(9));
        }
        std::cout << std::endl;

        // while (coroutine::_details::debug_get_active_promise_count() > 0) {
        //     std::cout << std::format("Active promises: {}\n", coroutine::_details::debug_get_active_promise_count()) << std::flush;
        //     std::this_thread::sleep_for(std::chrono::milliseconds(100));
        //     say1();
        // }


        std::cout << coroutine::_details::debug_get_active_promise_count() << std::endl;
    }

    std::cout << coroutine::_details::debug_get_active_promise_count() << std::endl;
}
