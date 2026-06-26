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

    co_await do_many_things(n - 1), co_await do_many_things(n - 2);

    co_await coroutine::all_of(do_many_things(n - 3), do_many_things(n - 4));

    // co_await coroutine::all_of(do_many_things(n - 2), do_many_things(n - 3));

    std::cout << n;
}

task<void> test_many_asm() {
    co_await coroutine::any_of(
        coroutine::any_of(coroutine::any_of(say1(), say1(), say1(), say1()), coroutine::any_of(say1(), say1())),
        coroutine::any_of(coroutine::any_of(coroutine::any_of(say1(), say1()), say1(), say1()),
                          coroutine::any_of(say1(), say1())));
    co_await coroutine::any_of(coroutine::any_of(say1(),
        coroutine::any_of(
            say1(),
            coroutine::any_of(say1(),
                coroutine::any_of(say1(),
                    coroutine::any_of(say1(),
                        coroutine::any_of(say1(),
                            coroutine::any_of(say1(),
                                coroutine::any_of(say1(),
                                    coroutine::any_of(say1(),
                                        coroutine::any_of(say1(),
                                            coroutine::any_of(say1(),
                                                coroutine::any_of(say1(),
                                                    say1()),
                                                say1()), say1()), say1()), say1()),
                                say1(), say1(), say1()),
                            say1(), say1(), say1()),
                        say1(), say1(), say1()),
                    say1(), say1(), say1())))));

    co_return;
}

int main() {
    for (int i = 0; i < 100000; ++i) {
        {
            std::cout << std::format("\nNow running test_many_asm() iteration {}\n", i) << std::endl;

            auto execution_ctx = coroutine::_details::multithreaded_execution_context{4};

            execution_ctx.block_on(test_many_asm());
        }
        // std::cout << std::endl;


        std::cout << coroutine::_details::debug_get_active_promise_count();
    }

    std::cout << "Heap allocations: " << std::endl;

    std::cout << coroutine::_details::promise_base::g_heap_alloc_count.load(std::memory_order_acquire) << std::endl;

    std::cout << "Active promises: " << std::endl;

    std::cout << coroutine::_details::debug_get_active_promise_count() << std::endl;
}
