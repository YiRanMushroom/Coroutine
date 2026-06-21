#include "coroutine.hpp"

using coroutine::task;
using coroutine::cancelable_task;

task<void> say1() {
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

cancelable_task<void> say_but_slow() {
    std::cout << "Starting slow task..." << std::endl;
    co_await coroutine::sleep_for(std::chrono::seconds(2));
    std::cout << "Finished slow task!" << std::endl;
    co_await say1();
    co_return;
}

cancelable_task<void> test_and() {
    co_await coroutine::all_of(say_but_slow(), say_but_slow(), say_but_slow());
    co_return;
}

task<void> test_or() {
    co_await coroutine::any_of<task>(say_but_slow().into(), say_but_slow().into(), say1());
}

int main() {
    {
        auto execution_ctx = coroutine::_details::multithreaded_execution_context{};

        // auto t = say3();

        execution_ctx.block_on(test_or());
    }
    std::cout << coroutine::_details::debug_get_active_promise_count() << std::endl;
}
