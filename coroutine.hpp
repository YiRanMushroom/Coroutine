#pragma once

#include <cassert>
#include <coroutine>
#include <atomic>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <variant>
#include <concepts>
#include <expected>
#include <future>
#include <ranges>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <format>
#include <thread>
#include <print>
#include <semaphore>
#if __has_include(<generator>)
#include <generator>
#endif

#include "thread_pool/thread_pool.h"

#define DO_HALO 1
#if DO_HALO
#if __clang__
#define COROUTINE_AWAIT_ELIDABLE [[clang::coro_await_elidable]]
#define COROUTINE_AWAIT_ELIDABLE_ARGUMENT [[clang::coro_await_elidable_argument]]
#else
#define COROUTINE_AWAIT_ELIDABLE
#define COROUTINE_AWAIT_ELIDABLE_ARGUMENT
#endif
#else
#define COROUTINE_AWAIT_ELIDABLE
#define COROUTINE_AWAIT_ELIDABLE_ARGUMENT
#endif

#if DO_DEBUG_PRINT
#define debug_print(...) \
    do { \
        std::println(__VA_ARGS__); \
    } while (0)
#else
#define debug_print(...)
#endif

#ifdef __clang__
#define MUST_INLINE [[clang::always_inline]]
#elifdef _MSC_VER
#define MUST_INLINE __forceinline
#else
#define MUST_INLINE [[gnu::always_inline]]
#endif

#ifdef __clang__
#define NO_ASAN  __attribute__((no_sanitize("address")))
#else
#define NO_ASAN
#endif

namespace coroutine {
    class execution_context;

    namespace _details {
        template<typename... Ts>
        struct overloaded : Ts... {
            using Ts::operator()...;
        };

        template<typename T>
        class task_base;

        class pin_resource_base {
        private:
            std::atomic_uint32_t m_strong_count{1};
            std::atomic_uint32_t m_weak_count{1};

        public:
            pin_resource_base() = default;

        protected:
            virtual void release_resources() noexcept = 0;

            virtual void destroy() noexcept = 0;

            ~pin_resource_base() {
                // std::cout << std::format("destroying pin_resource_base at {:p}\n", static_cast<void *>(this)) <<
                //         std::endl;
                debug_print("destroying pin_resource_base at {:p}", static_cast<void *>(this));
            }

        public:
            inline void release_strong_reference() noexcept {
                // __debugbreak();
                // std::cout << "release_strong_reference on " << static_cast<void *>(this) << std::endl;
                debug_print("release_strong_reference on {:p}", static_cast<void *>(this));


                // assert(m_strong_count.load(std::memory_order_relaxed) < UINT16_MAX);

                if (m_strong_count.load(std::memory_order_relaxed) >= UINT16_MAX) {
                    // __debugbreak();
                }

                uint32_t old_count = m_strong_count.fetch_sub(1, std::memory_order_release);

                assert(old_count > 0 && "Logic error: releasing weak reference when count is already zero.");

                if (old_count == 1) {
                    std::atomic_thread_fence(std::memory_order_acquire);
                    release_resources();
                    release_weak_reference();
                }
            }

            inline void release_weak_reference() noexcept {
                assert(m_weak_count.load(std::memory_order_relaxed) < UINT16_MAX);

                uint32_t old_count = m_weak_count.fetch_sub(1, std::memory_order_release);

                assert(old_count > 0 && "Logic error: releasing weak reference when count is already zero.");

                if (old_count == 1) {
                    std::atomic_thread_fence(std::memory_order_acquire);
                    destroy();
                }
            }

            inline void add_strong_reference() noexcept {
                m_strong_count.fetch_add(1, std::memory_order_relaxed);
            }

            inline void add_weak_reference() noexcept {
                m_weak_count.fetch_add(1, std::memory_order_relaxed);
            }

            inline bool lock() noexcept {
                uint32_t count = m_strong_count.load(std::memory_order_relaxed);
                while (count != 0) {
                    if (m_strong_count.compare_exchange_weak(count, count + 1, std::memory_order_acquire,
                                                             std::memory_order_relaxed)) {
                        return true;
                    }
                }
                return false;
            }
        };

        struct abstract_awaitable_base {
            // auto by_promise(promise_base *promise);
            // auto await_resume
        };

        template<typename T>
        concept is_awaitable = std::derived_from<std::remove_cvref_t<T>, abstract_awaitable_base>;

        inline namespace resource_acquisition_semantics {
            struct adopt {};

            struct copy {};
        }

        class ref_counted_resource_weak_handle;

        class ref_counted_resource_handle {
            friend ref_counted_resource_weak_handle;

        protected:
            inline static std::unordered_set<ref_counted_resource_handle *> g_active_handles;
            inline static std::recursive_mutex g_active_handles_mutex;

            void register_handle() noexcept {
                // std::lock_guard<std::recursive_mutex> lock(g_active_handles_mutex);
                // g_active_handles.insert(this);
            }

            void unregister_handle() noexcept {
                // std::lock_guard<std::recursive_mutex> lock(g_active_handles_mutex);
                // if (g_active_handles.find(this) != g_active_handles.end()) {
                //     g_active_handles.erase(this);
                // } else {
                //     // std::cout << std::format("Error: Access invalid location {:p} in ref_counted_resource_handle::unregister_handle().\n",
                //     //                          static_cast<void *>(this)) << std::endl;
                //     debug_print(
                //         "Error: Access invalid location {:p} in ref_counted_resource_handle::unregister_handle().",
                //         static_cast<void *>(this));
                //     __debugbreak();
                // }
            }

        public:
            ref_counted_resource_handle() noexcept {
                register_handle();
            }

            void print_handle_created() const noexcept {
                // std::cout << std::format("Handle {} to resource at {} is being created.",
                //                          static_cast<const void *>(this), static_cast<void *>(m_resource)) << std::endl;
            }

            ref_counted_resource_handle(pin_resource_base *resource,
                                        resource_acquisition_semantics::adopt) noexcept : m_resource(resource) {
                print_handle_created();
                register_handle();
            }

            ref_counted_resource_handle(pin_resource_base *resource,
                                        resource_acquisition_semantics::copy) noexcept : m_resource(resource) {
                register_handle();
                if (m_resource) {
                    m_resource->add_strong_reference();
                    print_handle_created();
                }
            }

            ref_counted_resource_handle(const ref_counted_resource_handle &other) noexcept : m_resource(
                other.m_resource) {
                register_handle();
                if (m_resource) {
                    m_resource->add_strong_reference();
                    print_handle_created();
                }
            }

            ref_counted_resource_handle(ref_counted_resource_handle &&other) noexcept : m_resource(
                std::exchange(other.m_resource, nullptr)) {
                register_handle();
            }

            ref_counted_resource_handle &operator=(const ref_counted_resource_handle &other) noexcept {
                if (this != &other) {
                    if (m_resource) {
                        m_resource->release_strong_reference();
                    }
                    m_resource = other.m_resource;
                    if (m_resource) {
                        m_resource->add_strong_reference();
                    }
                }
                return *this;
            }

            ref_counted_resource_handle &operator=(ref_counted_resource_handle &&other) noexcept {
                if (this != &other) {
                    pin_resource_base *old_resource = m_resource;
                    m_resource = std::exchange(other.m_resource, nullptr);
                    if (old_resource) {
                        old_resource->release_strong_reference();
                    }
                }
                return *this;
            }

            ~ref_counted_resource_handle() noexcept {
                unregister_handle();
                // std::cout << std::format("Handle {} to resource at {} is being destroyed.",
                //          static_cast<void *>(this), static_cast<void *>(m_resource)) << std::endl;

                if (auto resource = std::exchange(m_resource, nullptr)) {
                    resource->release_strong_reference();
                }
            }

            explicit operator bool() const noexcept {
                return m_resource != nullptr;
            }

            [[nodiscard]] bool has_value() const noexcept {
                return static_cast<bool>(*this);
            }

            void reset() noexcept {
                if (m_resource) {
                    auto *resource = std::exchange(m_resource, nullptr);
                    // this must be done before releasing the strong reference to avoid potential use-after-free issues
                    resource->release_strong_reference();
                }
            }

            [[nodiscard]] pin_resource_base *get() const noexcept {
                return m_resource;
            }

        private:
            pin_resource_base *m_resource = nullptr;
        };

        class ref_counted_resource_weak_handle {
        public:
            ref_counted_resource_weak_handle() noexcept = default;

            ref_counted_resource_weak_handle(pin_resource_base *resource,
                                             resource_acquisition_semantics::copy) noexcept : m_resource(resource) {
                if (m_resource) {
                    m_resource->add_weak_reference();
                }
            }

            ref_counted_resource_weak_handle(const ref_counted_resource_handle &strong_handle) noexcept : m_resource(
                strong_handle.m_resource) {
                if (m_resource) {
                    m_resource->add_weak_reference();
                }
            }

            ref_counted_resource_weak_handle(const ref_counted_resource_weak_handle &other) noexcept : m_resource(
                other.m_resource) {
                if (m_resource) {
                    m_resource->add_weak_reference();
                }
            }

            ref_counted_resource_weak_handle(ref_counted_resource_weak_handle &&other) noexcept : m_resource(
                std::exchange(other.m_resource, nullptr)) {}

            ref_counted_resource_weak_handle &operator=(const ref_counted_resource_weak_handle &other) noexcept {
                if (this != &other) {
                    if (m_resource) {
                        m_resource->release_weak_reference();
                    }
                    m_resource = other.m_resource;
                    if (m_resource) {
                        m_resource->add_weak_reference();
                    }
                }
                return *this;
            }

            ref_counted_resource_weak_handle &operator=(ref_counted_resource_weak_handle &&other) noexcept {
                if (this != &other) {
                    pin_resource_base *old_resource = m_resource;
                    m_resource = std::exchange(other.m_resource, nullptr);
                    if (old_resource) {
                        old_resource->release_weak_reference();
                    }
                }
                return *this;
            }

            ~ref_counted_resource_weak_handle() noexcept {
                if (auto resource = std::exchange(m_resource, nullptr)) {
                    resource->release_weak_reference();
                }
            }

            [[nodiscard]] ref_counted_resource_handle lock() const noexcept {
                if (m_resource && m_resource->lock()) {
                    return ref_counted_resource_handle(m_resource, resource_acquisition_semantics::adopt{});
                }
                return {};
            }

            void reset() noexcept {
                if (m_resource) {
                    m_resource->release_weak_reference();
                    m_resource = nullptr;
                }
            }

            [[nodiscard]] bool is_null() const noexcept {
                return m_resource == nullptr;
            }

            [[nodiscard]] pin_resource_base *get_raw() const noexcept {
                return m_resource;
            }

        private:
            pin_resource_base *m_resource = nullptr;
        };

        class promise_base;

        class future_base {
        protected:
            [[nodiscard]] promise_base *get_promise_base() const noexcept;

        protected:
            ref_counted_resource_handle m_coroutine_handle;

        public:
            future_base() noexcept = default;

            future_base(ref_counted_resource_handle coroutine_handle) noexcept : m_coroutine_handle(
                std::move(coroutine_handle)) {}

            future_base(const future_base &) = delete;

            future_base &operator=(const future_base &) = delete;

            future_base(future_base &&other) noexcept : m_coroutine_handle(std::move(other.m_coroutine_handle)) {}

            future_base &operator=(future_base &&other) noexcept {
                if (this != &other) {
                    m_coroutine_handle = std::move(other.m_coroutine_handle);
                }
                return *this;
            }

            ~future_base() noexcept = default;

            explicit operator bool() const noexcept {
                return m_coroutine_handle.has_value();
            }

            [[nodiscard]] bool has_value() const noexcept {
                return static_cast<bool>(*this);
            }

            void reset() noexcept {
                m_coroutine_handle.reset();
            }

            ref_counted_resource_handle release_handle() noexcept {
                return std::move(m_coroutine_handle);
            }
        };

        inline std::atomic_size_t g_promise_count{0};

        inline size_t debug_get_active_promise_count() noexcept {
            return g_promise_count.load();
        }

        class promise_base : public pin_resource_base {
            template<typename T>
            friend class task_base;

        protected:
            ~promise_base() {
                // std::cout << std::format("destroying promise_base at {:p}\n", static_cast<void *>(this)) << std::endl;

                // __debugbreak();

                // __debugbreak();

                if (m_pinned_self) {
                    debug_print("Error: promise_base at {:p} is being destroyed while still pinned.",
                                static_cast<void *>(this));
                }

                g_promise_count.fetch_sub(1);
            }

        public:
            promise_base() {
                g_promise_count.fetch_add(1);
            }

            promise_base(const promise_base &) = delete;

            promise_base &operator=(const promise_base &) = delete;

            promise_base(promise_base &&) = delete;

            promise_base &operator=(promise_base &&) = delete;

            template<is_awaitable Awaitable>
            decltype(auto) await_transform(Awaitable &&awaitable) {
                return std::forward<Awaitable>(awaitable).by_promise(this);
            }

            template<typename T> requires (!is_awaitable<T>)
            decltype(auto) await_transform(T &&value) {
                return std::forward<T>(value);
            }

            std::function<void()> get_continuation() && noexcept {
                // std::lock_guard<std::mutex> lock(m_continuation_protector);
                if (m_in_continuation_access.fetch_add(1) != 0) {
                    __debugbreak();
                }
                auto ret = std::exchange(m_continuation, nullptr);
                m_in_continuation_access.fetch_sub(1);
                return ret;
            }

        public:
            inline void set_continuation(std::function<void()> continuation) {
                // std::lock_guard<std::mutex> lock(m_continuation_protector);
                if (m_in_continuation_access.fetch_add(1) != 0) {
                    __debugbreak();
                }
                m_continuation = std::move(continuation);
                m_in_continuation_access.fetch_sub(1);
            }

            inline void set_execution_context(execution_context *context) {
                m_execution_context = context;
            }

            inline void pin() {
                if (m_pinned_self) {
                    return;
                }
                m_pinned_self = ref_counted_resource_handle(this, resource_acquisition_semantics::copy{});
            }

            inline ref_counted_resource_handle borrow() const noexcept {
                return ref_counted_resource_handle(const_cast<promise_base *>(this),
                                                   resource_acquisition_semantics::copy{});
            }

            inline ref_counted_resource_weak_handle weak_borrow() const noexcept {
                return ref_counted_resource_weak_handle(const_cast<promise_base *>(this),
                                                        resource_acquisition_semantics::copy{});
            }

            inline void unpin() {
                m_pinned_self.reset();
            }

        protected:
            void release_resources() noexcept override {
                auto weak_self = weak_borrow();
                m_pinned_self.reset();
            }

            inline void destroy() noexcept override {
                // std::cout << "destroying promise at " << self_addr << std::endl;

                // this->unpin();

                assert(!m_pinned_self);

                // std::cout << std::format("m_pinned_self is :{}", (void *) this->m_pinned_self.get()) << std::endl;

                // std::cout << std::format("m_coroutine_handle is :{}", this->m_coroutine_handle.address()) << std::endl;

                auto moved_parent = std::move(this->m_parent_handle);
                this->m_parent_handle.reset();

                this->m_coroutine_handle.destroy();
                // std::cout << "destroyed promise at " << self_addr << std::endl;
            }

        protected:
            void finalize_finished_promise() {
                this->unpin();
                auto moved = std::move(*this).get_continuation();
                if (moved) {
                    moved();
                }
            }

        public:
            inline void on_finished() {
                finalize_finished_promise();
            }

        protected:
            std::function<void()> m_continuation;
            ref_counted_resource_handle m_pinned_self;
            execution_context *m_execution_context = nullptr;
            ref_counted_resource_weak_handle m_parent_handle{};
            std::coroutine_handle<> m_coroutine_handle;
            std::atomic<int> m_in_continuation_access{0};
            std::atomic_bool m_finalized{false};
            bool cancelable = false;

        public:
            inline bool should_try_finalize() noexcept {
                return m_finalized.exchange(true, std::memory_order_acq_rel) == false;
            }

            inline bool is_cancelable() const noexcept {
                return cancelable;
            }

            inline void set_cancelable(bool value) noexcept {
                cancelable = value;
            }

            void set_parent(ref_counted_resource_weak_handle parent) noexcept {
                m_parent_handle = std::move(parent);
            }

            execution_context *get_execution_context() const noexcept {
                return m_execution_context;
            }

            constexpr static std::suspend_always initial_suspend() noexcept {
                return {};
            }

            std::coroutine_handle<> get_coroutine_handle() const noexcept {
                return m_coroutine_handle;
            }

            struct return_object_builder {
                promise_base *m_promise;

                template<typename Task>
                operator Task() {
                    assert(m_promise);
                    return Task(ref_counted_resource_handle(std::exchange(m_promise, nullptr),
                                                            resource_acquisition_semantics::adopt{}));
                }
            };

            template<typename T>
            return_object_builder get_return_object(this T &self) {
                self.m_coroutine_handle = std::coroutine_handle<T>::from_promise(self);

                return {static_cast<promise_base *>(&self)};
            }

            struct final_awaiter_type : std::suspend_always {
                promise_base *m_promise;

                void await_suspend(std::coroutine_handle<> h) const noexcept {
                    if (m_promise && m_promise->should_try_finalize()) {
                        m_promise->on_finished();
                    }
                }
            };

            final_awaiter_type final_suspend() noexcept {
                return final_awaiter_type{.m_promise = this};
            }

        public:
            inline static std::atomic_size_t g_heap_alloc_count{0};

            void *operator new(std::size_t size) {
                g_heap_alloc_count.fetch_add(1, std::memory_order_relaxed);
                return ::operator new(size);
            }

            void operator delete(void *ptr) noexcept {
                return ::operator delete(ptr);
            }
        };
    }

    class execution_context {
    public:
        virtual ~execution_context() = default;

        virtual void resume_promise_weak(_details::promise_base *promise) = 0;

        template<template<typename...> typename TaskType, typename T>
        NO_ASAN T block_on(TaskType<T> task);

        template<template<typename...> typename TaskType, typename T>
        std::future<T> async_execute(TaskType<T> task);

        virtual void wait_idle() = 0;
    };


    namespace _details {
        inline promise_base *future_base::get_promise_base() const noexcept {
            if (m_coroutine_handle) {
                return static_cast<promise_base *>(m_coroutine_handle.get()); // NOLINT(*-pro-type-static-cast-downcast)
            }
            return nullptr;
        }

        inline namespace result_state {
            struct not_finished {};

            template<typename T>
            using finished = std::conditional_t<std::is_void_v<T>, std::monostate, T>;

            using uncaught_exception = std::exception_ptr;
        }

        template<typename T>
        class promise : public promise_base {
        public:
            ~promise() = default;

        public:
            promise() = default;

        public:
            using value_type = T;

            T get_result_copy() const {
                // std::lock_guard lock(m_result_mutex);
                return std::visit(overloaded{
                                      [](const not_finished &) -> T {
                                          throw std::runtime_error("Result not set.");
                                      },
                                      [](const finished<T> &result) -> T {
                                          return result;
                                      },
                                      [](const uncaught_exception &e) -> T {
                                          std::rethrow_exception(e);
                                      }
                                  }, m_result);
            }

            T get_result_move() {
                // std::lock_guard lock(m_result_mutex);
                return std::visit(overloaded{
                                      [](const not_finished &) -> T {
                                          throw std::runtime_error("Result not set.");
                                      },
                                      [](finished<T> &result) -> T {
                                          return std::move(result);
                                      },
                                      [](const uncaught_exception &e) -> T {
                                          std::rethrow_exception(e);
                                      }
                                  }, m_result);
            }

        private:
            std::variant<not_finished, finished<T>, uncaught_exception> m_result{not_finished{}};
            // mutable std::recursive_mutex m_result_mutex;

        public:
            template<std::convertible_to<T> U>
            void return_value(U value) {
                {
                    // std::lock_guard lock(m_result_mutex);
                    m_result.template emplace<finished<T>>(std::move(value));
                }
            }

            void unhandled_exception() {
                {
                    // std::lock_guard lock(m_result_mutex);
                    m_result.template emplace<uncaught_exception>(std::current_exception());
                }
            }
        };

        template<>
        class promise<void> : public promise_base {
        public:
            ~promise() = default;

        public:
            promise() = default;

        public:
            using value_type = void;

            void get_result_copy() const {
                // std::lock_guard lock(m_result_mutex);
                return std::visit(overloaded{
                                      [](const not_finished &) -> void {
                                          throw std::runtime_error("Result not set.");
                                      },
                                      [](const finished<void> &) -> void {
                                          // No value to return for void, just indicate success.
                                      },
                                      [](const uncaught_exception &e) -> void {
                                          std::rethrow_exception(e);
                                      }
                                  }, m_result);
            }

            void get_result_move() {
                // std::lock_guard lock(m_result_mutex);
                return get_result_copy();
            }

            void return_void() {
                {
                    // std::lock_guard lock(m_result_mutex);
                    m_result.template emplace<finished<void>>(std::monostate{});
                }
            }

            void unhandled_exception() {
                {
                    // std::lock_guard lock(m_result_mutex);
                    m_result.template emplace<uncaught_exception>(std::current_exception());
                }
            }

        private:
            std::variant<not_finished, finished<void>, uncaught_exception> m_result{not_finished{}};
            // mutable std::recursive_mutex m_result_mutex;

        public:
        };

        struct task_base_flag {};

        template<typename T>
        class COROUTINE_AWAIT_ELIDABLE task_base : public abstract_awaitable_base, public future_base,
                                                   public std::suspend_always, public task_base_flag {
        public:
            using promise_type = promise<T>;

            using value_type = T;

            ~task_base() = default;

            task_base() = default;

            task_base(const task_base &) = delete;

            task_base &operator=(const task_base &) = delete;

            task_base(task_base &&) = default;

            task_base &operator=(task_base &&) = default;

            task_base(ref_counted_resource_handle coroutine_handle) noexcept : future_base(
                std::move(coroutine_handle)) {}

            [[nodiscard]] promise<T> *get_promise() const {
                promise_base *base = this->get_promise_base();
                return static_cast<promise<T> *>(base);
            }

            T get_result() const & {
                if (!this->m_coroutine_handle) {
                    throw std::runtime_error("No coroutine associated with this task.");
                }
                promise<T> *promise = get_promise();
                return promise->get_result_copy();
            }

            T get_result() && {
                if (!this->m_coroutine_handle) {
                    throw std::runtime_error("No coroutine associated with this task.");
                }
                promise<T> *promise = get_promise();
                return promise->get_result_move();
            }

            auto by_promise(promise_base *parent_promise) {
                assert(parent_promise);

                if (!this->m_coroutine_handle) {
                    throw std::runtime_error("No coroutine associated with this task.");
                }

                struct awaiter {
                    task_base<T> *m_task;
                    promise_base *m_parent_promise;

                    // std::binary_semaphore m_submitted{0};

                    [[nodiscard]] constexpr static bool await_ready() noexcept {
                        return false;
                    }

                    void await_suspend(std::coroutine_handle<> h) noexcept {
                        execution_context *context = m_parent_promise->get_execution_context();
                        assert(context);

                        promise_base *promise = m_task->get_promise();
                        if (!promise->get_execution_context()) {
                            promise->set_execution_context(context);
                        }

                        auto weak_parent = m_parent_promise->weak_borrow();

                        promise->set_parent(weak_parent);

                        auto *local_ctx = promise->get_execution_context();

                        promise->set_continuation([weak_parent = std::move(weak_parent)]() mutable {
                            auto parent = weak_parent.lock();
                            if (!parent) {
                                return;
                            }
                            auto *p = static_cast<promise_base *>(parent.get());
                            p->get_execution_context()->resume_promise_weak(p);
                        });

                        local_ctx->resume_promise_weak(promise);

                        // m_submitted.release();
                    }

                    auto await_resume() {
                        // m_submitted.acquire();
                        return m_task->get_promise()->get_result_move();
                    }
                };

                return awaiter{this, parent_promise};
            }

            T await_resume() {
                if (!this->m_coroutine_handle) {
                    throw std::runtime_error("No coroutine associated with this task.");
                }
                promise<T> *promise = get_promise();
                return promise->get_result_move();
            }

            template<typename Self>
            decltype(auto) with_context(this Self &&self, execution_context *ctx) {
                self.get_promise()->set_execution_context(ctx);
                return std::forward<Self>(self);
            }

            [[nodiscard]] const ref_counted_resource_handle &get_coroutine_handle() const noexcept {
                return this->m_coroutine_handle;
            }

            ref_counted_resource_handle &get_coroutine_handle() noexcept {
                return this->m_coroutine_handle;
            }

            ref_counted_resource_handle release_handle() && noexcept {
                return std::move(this->m_coroutine_handle);
            }

            template<template<typename> typename TaskType>
            TaskType<T> into(
                COROUTINE_AWAIT_ELIDABLE_ARGUMENT
                this task_base<T> &&self) {
                return TaskType<T>(std::move(self).release_handle());
            }
        };

        template<typename T>
        class task;

        template<typename T>
        class COROUTINE_AWAIT_ELIDABLE cancelable_task : public task_base<T> {
        public:
            using typename task_base<T>::promise_type;

            cancelable_task(ref_counted_resource_handle &&coroutine_handle) noexcept : task_base<T>(
                std::move(coroutine_handle)) {
                auto *promise = this->get_promise();
                assert(promise);
                promise->set_cancelable(true);
            }

            void cancel() {
                this->m_coroutine_handle.reset();
            }

            cancelable_task() = default;

            cancelable_task(const cancelable_task &) = delete;

            cancelable_task &operator=(const cancelable_task &) = delete;

            cancelable_task(cancelable_task &&) = default;

            cancelable_task &operator=(cancelable_task &&) = default;
        };

        template<typename T>
        class COROUTINE_AWAIT_ELIDABLE task : public task_base<T> {
        public:
            using typename task_base<T>::promise_type;

            task(ref_counted_resource_handle &&coroutine_handle) noexcept : task_base<T>(std::move(coroutine_handle)) {}

            task() = default;

            task(const task &) = delete;

            task &operator=(const task &) = delete;

            task(task &&) = default;

            task &operator=(task &&) = default;

            template<std::derived_from<task_base<T>>>
            task(task_base<T> &&base) noexcept : task<T>(std::move(base).release_handle()) {}
        };

        struct get_context_type : abstract_awaitable_base, std::suspend_never {
            get_context_type &by_promise(promise_base *promise) {
                assert(promise);
                m_context = promise->get_execution_context();
                return *this;
            }

            [[nodiscard]] execution_context *await_resume() const noexcept {
                return m_context;
            }

        private:
            execution_context *m_context = nullptr;
        };

        inline get_context_type get_current_coroutine_context() {
            return {};
        }

        class multithreaded_execution_context : public execution_context {
        public:
            explicit multithreaded_execution_context(uint32_t thread_count = std::jthread::hardware_concurrency() * 2)
                : m_thread_count(thread_count),
                  m_thread_pool(std::make_unique<dp_thread_pool::thread_pool<>>(thread_count)) {}


            void resume_promise_weak(_details::promise_base *promise) override {
                auto weak_handle = promise->weak_borrow();
                m_thread_pool->enqueue_detach([_weak_handle = std::move(weak_handle)] mutable {
                    auto weak_handle = std::move(_weak_handle);
                    auto locked = weak_handle.lock();
                    if (!locked) {
                        return;
                    }
                    auto *promise = static_cast<promise_base *>(locked.get());
                    const std::coroutine_handle<> handle = promise->get_coroutine_handle();

                    if (handle && !handle.done()) {
                        handle.resume();
                    } else {
                        __debugbreak();
                    }

                    // if (handle.done() && promise->should_try_finalize()) {
                    // }
                });
            }

            ~multithreaded_execution_context() override = default;


            void wait_idle() override {
                m_thread_pool->wait_for_tasks();
                m_thread_pool->clear_tasks();
            }

        private:
            size_t m_thread_count = 0;
            std::unique_ptr<dp_thread_pool::thread_pool<>> m_thread_pool;
        };
    }

    template<template<typename...> typename TaskType, typename T>
    NO_ASAN T execution_context::block_on(TaskType<T> task) {
        std::condition_variable cv;
        std::mutex mtx;
        bool finished = false;

        std::function<void()> continuation = [&] {
            std::lock_guard lock(mtx);
            finished = true;
            cv.notify_one();
        };

        auto *promise = task.get_promise();
        assert(promise);
        promise->set_continuation(continuation);
        promise->set_execution_context(this);

        resume_promise_weak(promise);

        this->wait_idle();

        std::unique_lock lock(mtx);
        cv.wait(lock, [&] { return finished; });

        return task.get_result();
    }

    template<template <typename...> class TaskType, typename T>
    std::future<T> execution_context::async_execute(TaskType<T> task) {
        return std::async(std::launch::async, [this, task = std::move(task)]() mutable {
            return this->block_on(std::move(task));
        });
    }

    template<typename T>
    using task = _details::task<T>;

    template<typename T>
    using cancelable_task = _details::cancelable_task<T>;

    namespace _details {
        struct get_promise_type : abstract_awaitable_base, std::suspend_never {
            get_promise_type &by_promise(promise_base *promise) {
                assert(promise);
                m_promise = promise;
                return *this;
            }

            [[nodiscard]] promise_base *await_resume() const noexcept {
                return m_promise;
            }

        private:
            promise_base *m_promise = nullptr;
        };

        inline get_promise_type get_current_promise_base() {
            return {};
        }

        struct coroutine_info {
            execution_context *context;
            promise_base *self;
        };

        struct get_coroutine_info_type : abstract_awaitable_base, std::suspend_never {
            get_coroutine_info_type &by_promise(promise_base *promise) {
                assert(promise);
                m_info = {promise->get_execution_context(), promise};
                return *this;
            }

            [[nodiscard]] coroutine_info await_resume() const noexcept {
                return m_info;
            }

        private:
            coroutine_info m_info{};
        };

        inline get_coroutine_info_type get_current_coroutine_info() { return {}; }


        template<template<typename...> typename TaskType, typename T, typename... Rest>
        TaskType<std::expected<T, std::exception_ptr>, Rest...> into_non_throw(
            COROUTINE_AWAIT_ELIDABLE_ARGUMENT TaskType<T, Rest...> _task) {
            auto task = std::move(_task);
            try {
                if constexpr (std::is_same_v<T, void>) {
                    co_await std::move(task);
                    co_return std::expected<void, std::exception_ptr>{};
                } else {
                    co_return std::expected<T, std::exception_ptr>{co_await std::move(task)};
                }
            } catch (...) {
                co_return std::unexpected{std::current_exception()};
            }
        }

        template<typename T, typename S>
        class ex_promise;

        template<typename T, typename S>
        class COROUTINE_AWAIT_ELIDABLE ex_task : public task_base<T> {
        public:
            using promise_type = ex_promise<T, S>;
            using typename task_base<T>::value_type;

            ex_task() = default;

            ex_task(ref_counted_resource_handle &&coroutine_handle) noexcept : task_base<T>(
                std::move(coroutine_handle)) {}

            ex_promise<T, S> *get_ex_promise() const {
                return static_cast<ex_promise<T, S> *>(this->get_promise_base());
            }
        };

        template<typename T, typename S>
        class ex_promise : public promise<T> {
        public:
            S state;

        protected:
            void release_resources() noexcept override {
                if constexpr (requires { state.release_resources(); }) {
                    state.release_resources();
                }
                promise<T>::release_resources();
            }
        };

        struct get_current_promise_t : abstract_awaitable_base, std::suspend_never {
            promise_base *m_promise = nullptr;

            get_current_promise_t &by_promise(promise_base *promise) {
                assert(promise);
                m_promise = promise;
                return *this;
            }

            [[nodiscard]] promise_base *await_resume() const noexcept {
                return m_promise;
            }
        };

        inline get_current_promise_t get_current_promise() {
            return {};
        }

        struct COROUTINE_AWAIT_ELIDABLE trivial_single_use_task {
            struct promise_type {
                trivial_single_use_task get_return_object() {
                    return trivial_single_use_task{std::coroutine_handle<promise_type>::from_promise(*this)};
                }

                constexpr static std::suspend_never initial_suspend() noexcept {
                    return {};
                }

                constexpr static std::suspend_never final_suspend() noexcept {
                    return {};
                }

                constexpr static void unhandled_exception() noexcept {
                    std::unreachable();
                }
            };

            std::coroutine_handle<> handle;
        };

        template<typename Func>
            requires std::is_invocable_v<Func, std::coroutine_handle<>>
        struct suspend_and_then_t : public std::suspend_always {
            Func m_callback; // Func must be static and cannot capture any references
            std::binary_semaphore m_semaphore{0};

            suspend_and_then_t(Func &&callback) : m_callback(std::move(callback)) {}

            void await_suspend(std::coroutine_handle<> h) noexcept {
                m_callback(h);
                m_semaphore.release();
            }

            void await_resume() noexcept {
                m_semaphore.acquire();
            }
        };

        auto suspend_and_then(auto &&callback) {
            return suspend_and_then_t(std::forward<decltype(callback)>(callback));
        }

        template<typename T>
        struct COROUTINE_AWAIT_ELIDABLE immediate_value_task : std::suspend_never {
            struct promise_type {
                T value{};

                immediate_value_task get_return_object() {
                    return immediate_value_task{std::coroutine_handle<promise_type>::from_promise(*this)};
                }

                void return_value(COROUTINE_AWAIT_ELIDABLE_ARGUMENT T v) {
                    value = std::move(v);
                }

                constexpr static std::suspend_never initial_suspend() noexcept {
                    return {};
                }

                constexpr static std::suspend_always final_suspend() noexcept {
                    return {};
                }

                constexpr static void unhandled_exception() noexcept {
                    std::unreachable();
                }
            };

            std::coroutine_handle<promise_type> coro;

            immediate_value_task(std::coroutine_handle<promise_type> h) : coro(h) {}

            immediate_value_task(const immediate_value_task &) = delete;

            immediate_value_task &operator=(const immediate_value_task &) = delete;

            immediate_value_task(immediate_value_task &&other) = delete;

            immediate_value_task &operator=(immediate_value_task &&other) = delete;

            immediate_value_task() = delete;

            T await_resume() {
                assert(coro && coro.done());
                return std::move(coro.promise().value);
            }

            ~immediate_value_task() {
                assert(coro && coro.done());
                coro.destroy();
            }
        };

        template<typename Task>
        immediate_value_task<Task> force_await_embed_in_current_frame(
            COROUTINE_AWAIT_ELIDABLE_ARGUMENT
            Task task) {
            co_return task;
        }

        template<typename... T>
        struct all_of_args_shared_state {
            std::atomic_size_t remaining{sizeof...(T)};
            std::tuple<task_base<std::expected<T, std::exception_ptr>>...> non_throw_tasks;
        };

        template<typename... Tasks> requires (sizeof...(Tasks) > 0)
        ex_task<std::tuple<std::expected<typename Tasks::value_type, std::exception_ptr>...>, all_of_args_shared_state<
            Tasks...>> all_of(
            COROUTINE_AWAIT_ELIDABLE_ARGUMENT
            Tasks... tasks) {
            using RetType = std::tuple<std::expected<typename Tasks::value_type, std::exception_ptr>...>;
            using promise_type = ex_promise<RetType, all_of_args_shared_state<typename Tasks::value_type...>>;
            using stored_state_type = all_of_args_shared_state<typename Tasks::value_type...>;

            promise_type *promise = static_cast<promise_type *>(co_await get_current_promise());
            stored_state_type *state = &promise->state;

            state->non_throw_tasks = std::make_tuple(
                (co_await force_await_embed_in_current_frame(into_non_throw(std::move(tasks)))).with_context(
                    tasks.get_promise()->get_execution_context())...);

            co_await suspend_and_then(
                [_promise = promise](std::coroutine_handle<> h) {
                    auto promise = static_cast<promise_type *>(_promise);
                    auto self = promise->borrow();
                    auto state = &promise->state;
                    execution_context *exec_ctx = promise->get_execution_context();
                    auto parent = promise->weak_borrow();

                    std::array<promise_base *, sizeof...(Tasks)> to_start;

                    [&]<size_t... Is>(std::index_sequence<Is...>) {
                        ([&]<size_t idx>(std::integral_constant<size_t, idx>) {
                            promise_base *task_promise = std::get<idx>(state->non_throw_tasks).get_promise();
                            to_start[idx] = task_promise;

                            task_promise->set_parent(parent);
                            if (task_promise->get_execution_context() == nullptr) {
                                task_promise->set_execution_context(exec_ctx);
                            }
                            task_promise->set_continuation([_parent = parent] mutable {
                                auto parent = std::move(_parent);
                                auto locked_parent = parent.lock();
                                if (!locked_parent) {
                                    return;
                                }

                                auto *parent_promise = static_cast<promise_type *>(locked_parent.get());
                                auto all_of_state = &parent_promise->state;

                                if (all_of_state->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                                    parent_promise->get_execution_context()->resume_promise_weak(parent_promise);
                                }
                            });
                        }(std::integral_constant<size_t, Is>{}), ...);
                    }(std::index_sequence_for<typename Tasks::value_type...>{});

                    for (promise_base *task_promise: to_start) {
                        exec_ctx->resume_promise_weak(task_promise);
                    }
                });

            RetType ret_val = [state]<size_t... Is>(std::index_sequence<Is...>) {
                return std::make_tuple([state]<size_t I>(std::integral_constant<size_t, I>) {
                    auto task_moved = std::move(std::get<I>(state->non_throw_tasks));
                    return task_moved.get_promise()->get_result_move();
                }(std::integral_constant<size_t, Is>{})...);
            }(std::index_sequence_for<typename Tasks::value_type...>{});

            co_return ret_val;
        }

        template<typename... T>
        struct any_of_args_shared_state {
            std::atomic_bool finished{false};
            std::tuple<task_base<std::expected<T, std::exception_ptr>>...> non_throw_tasks;
            std::tuple<std::optional<std::expected<T, std::exception_ptr>>...> result;
        };

        template<typename... Tasks> requires (sizeof...(Tasks) > 0)
        ex_task<std::tuple<std::optional<std::expected<typename Tasks::value_type, std::exception_ptr>>...>,
            any_of_args_shared_state<typename Tasks::value_type...>> any_of(
            COROUTINE_AWAIT_ELIDABLE_ARGUMENT
            Tasks... tasks) {
            using promise_type = ex_promise<std::tuple<std::optional<std::expected<typename Tasks::value_type,
                    std::exception_ptr>>...>,
                any_of_args_shared_state<typename Tasks::value_type...>>;

            promise_type *promise = static_cast<promise_type *>(co_await get_current_promise());
            using state_type = any_of_args_shared_state<typename Tasks::value_type...>;
            state_type *state = &promise->state;

            state->non_throw_tasks = std::make_tuple(
                (co_await force_await_embed_in_current_frame(into_non_throw(std::move(tasks)))).with_context(
                    tasks.get_promise()->get_execution_context())...);

            co_await suspend_and_then(
                [promise_temp = promise](std::coroutine_handle<> h) {
                    promise_base *promise = static_cast<promise_base *>(promise_temp);
                    execution_context *exec_ctx = promise->get_execution_context();
                    auto parent = promise->weak_borrow();
                    state_type *state = &static_cast<promise_type *>(promise)->state;

                    std::array<promise_base *, sizeof...(Tasks)> to_start;

                    [&]<size_t... Is>(std::index_sequence<Is...>) {
                        ([&]<size_t idx>(std::integral_constant<size_t, idx>) {
                            promise_base *task_promise = std::get<idx>(state->non_throw_tasks).get_promise();
                            to_start[idx] = task_promise;

                            task_promise->set_parent(parent);
                            if (task_promise->get_execution_context() == nullptr) {
                                task_promise->set_execution_context(exec_ctx);
                            }
                            task_promise->set_continuation([parent, state]() mutable {
                                auto weak_parent = std::move(parent);
                                auto this_task = std::move(std::get<idx>(state->non_throw_tasks));
                                auto locked_parent = weak_parent.lock();
                                if (!locked_parent) {
                                    return;
                                }

                                auto *parent_promise = static_cast<promise_type *>(locked_parent.get());

                                if (!state->finished.exchange(true, std::memory_order_acq_rel)) {
                                    std::get<idx>(parent_promise->state.result) = this_task.get_promise()->
                                            get_result_move();
                                    parent_promise->get_execution_context()->resume_promise_weak(parent_promise);
                                }
                            });
                        }(std::integral_constant<size_t, Is>{}), ...);
                    }(std::index_sequence_for<typename Tasks::value_type...>{});

                    for (promise_base *task_promise: to_start) {
                        exec_ctx->resume_promise_weak(task_promise);
                    }
                });

            co_return std::move(state->result);
        }

        template<typename T>
        struct all_of_vector_shared_state {
            std::atomic_size_t remaining{0};
            std::vector<task_base<std::expected<T, std::exception_ptr>>> non_throw_tasks;
        };

        template<typename Task>
        ex_task<std::vector<std::expected<typename Task::value_type, std::exception_ptr>>,
            all_of_vector_shared_state<typename Task::value_type>> all_of(
            COROUTINE_AWAIT_ELIDABLE_ARGUMENT
            std::vector<Task> tasks) {
            using T = typename Task::value_type;
            using RetType = std::vector<std::expected<T, std::exception_ptr>>;
            using promise_type = ex_promise<RetType, all_of_vector_shared_state<T>>;
            using stored_state_type = all_of_vector_shared_state<T>;

            promise_type *promise = static_cast<promise_type *>(co_await get_current_promise());
            stored_state_type *state = &promise->state;

            state->non_throw_tasks.reserve(tasks.size());
            for (auto &task: tasks) {
                execution_context *ctx = task.get_promise()->get_execution_context();
                state->non_throw_tasks.push_back(
                    (co_await force_await_embed_in_current_frame(into_non_throw(std::move(task)))).with_context(ctx));
            }
            state->remaining.store(state->non_throw_tasks.size(), std::memory_order_relaxed);

            co_await suspend_and_then(
                [_promise = promise](std::coroutine_handle<> h) {
                    auto promise = static_cast<promise_type *>(_promise);
                    auto self = promise->borrow();
                    auto state = &promise->state;
                    execution_context *exec_ctx = promise->get_execution_context();
                    auto parent = promise->weak_borrow();

                    std::vector<promise_base *> to_start;
                    to_start.reserve(state->non_throw_tasks.size());

                    for (size_t idx = 0; idx < state->non_throw_tasks.size(); ++idx) {
                        promise_base *task_promise = state->non_throw_tasks[idx].get_promise();
                        to_start.push_back(task_promise);

                        task_promise->set_parent(parent);
                        if (task_promise->get_execution_context() == nullptr) {
                            task_promise->set_execution_context(exec_ctx);
                        }
                        task_promise->set_continuation([_parent = parent] mutable {
                            auto parent = std::move(_parent);
                            auto locked_parent = parent.lock();
                            if (!locked_parent) {
                                return;
                            }

                            auto *parent_promise = static_cast<promise_type *>(locked_parent.get());
                            auto all_of_state = &parent_promise->state;

                            if (all_of_state->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                                parent_promise->get_execution_context()->resume_promise_weak(parent_promise);
                            }
                        });
                    }

                    for (promise_base *task_promise: to_start) {
                        exec_ctx->resume_promise_weak(task_promise);
                    }
                });

            RetType ret_val;
            ret_val.reserve(state->non_throw_tasks.size());
            for (auto &t: state->non_throw_tasks) {
                auto task_moved = std::move(t);
                ret_val.push_back(task_moved.get_promise()->get_result_move());
            }

            co_return ret_val;
        }

        template<typename T>
        struct any_of_vector_shared_state {
            std::atomic_bool finished{false};
            std::vector<task_base<std::expected<T, std::exception_ptr>>> non_throw_tasks;
            std::vector<std::optional<std::expected<T, std::exception_ptr>>> result;
        };

        template<typename Task>
        ex_task<std::vector<std::optional<std::expected<typename Task::value_type, std::exception_ptr>>>,
            any_of_vector_shared_state<typename Task::value_type>> any_of(
            COROUTINE_AWAIT_ELIDABLE_ARGUMENT
            std::vector<Task> tasks) {
            using T = typename Task::value_type;
            using promise_type = ex_promise<std::vector<std::optional<std::expected<T, std::exception_ptr>>>,
                any_of_vector_shared_state<T>>;

            promise_type *promise = static_cast<promise_type *>(co_await get_current_promise());
            using state_type = any_of_vector_shared_state<T>;
            state_type *state = &promise->state;

            state->non_throw_tasks.reserve(tasks.size());
            for (auto &task: tasks) {
                execution_context *ctx = task.get_promise()->get_execution_context();
                state->non_throw_tasks.push_back(
                    (co_await force_await_embed_in_current_frame(into_non_throw(std::move(task)))).with_context(ctx));
            }
            state->result.resize(state->non_throw_tasks.size());

            co_await suspend_and_then(
                [promise_temp = promise](std::coroutine_handle<> h) {
                    promise_base *promise = static_cast<promise_base *>(promise_temp);
                    execution_context *exec_ctx = promise->get_execution_context();
                    auto parent = promise->weak_borrow();
                    state_type *state = &static_cast<promise_type *>(promise)->state;

                    std::vector<promise_base *> to_start;
                    to_start.reserve(state->non_throw_tasks.size());

                    for (size_t idx = 0; idx < state->non_throw_tasks.size(); ++idx) {
                        promise_base *task_promise = state->non_throw_tasks[idx].get_promise();
                        to_start.push_back(task_promise);

                        task_promise->set_parent(parent);
                        if (task_promise->get_execution_context() == nullptr) {
                            task_promise->set_execution_context(exec_ctx);
                        }
                        task_promise->set_continuation([parent, state, idx]() mutable {
                            auto weak_parent = std::move(parent);
                            auto this_task = std::move(state->non_throw_tasks[idx]);
                            auto locked_parent = weak_parent.lock();
                            if (!locked_parent) {
                                return;
                            }

                            auto *parent_promise = static_cast<promise_type *>(locked_parent.get());

                            if (!state->finished.exchange(true, std::memory_order_acq_rel)) {
                                parent_promise->state.result[idx] = this_task.get_promise()->get_result_move();
                                parent_promise->get_execution_context()->resume_promise_weak(parent_promise);
                            }
                        });
                    }

                    for (promise_base *task_promise: to_start) {
                        exec_ctx->resume_promise_weak(task_promise);
                    }
                });

            co_return std::move(state->result);
        }

        template<typename T> requires(!std::same_as<T, void>)
        class generator_promise : public promise_base {
        public:
            std::optional<T> get_result_move() {
                return std::visit(overloaded{
                                      [](std::monostate &) -> std::optional<T> {
                                          return std::nullopt;
                                      },
                                      [](T &value) -> std::optional<T> {
                                          auto result = std::move(value);
                                          return result;
                                      },
                                      [](const std::exception_ptr &e) -> std::optional<T> {
                                          std::rethrow_exception(e);
                                      }
                                  }, m_current_value);
            }

            void return_void() {
                m_current_value.template emplace<std::monostate>();
            }

            void unhandled_exception() {
                m_current_value.template emplace<std::exception_ptr>(std::current_exception());
            }

            struct yield_awaiter : std::suspend_always {
                promise_base *m_promise;

                void await_suspend(std::coroutine_handle<> h) const noexcept {
                    if (auto moved = std::move(*m_promise).get_continuation()) {
                        moved();
                    }
                }
            };

            template<std::convertible_to<T> U>
            yield_awaiter yield_value(U &&value) {
                m_current_value.template emplace<T>(std::forward<U>(value));
                return yield_awaiter{.m_promise = this};
            }

            [[nodiscard]] bool is_done() const noexcept {
                return this->get_coroutine_handle().done();
            }

        private:
            std::variant<std::monostate, T, std::exception_ptr> m_current_value{std::monostate{}};
        };

        template<typename T> requires(!std::same_as<T, void>)
        class generator : public future_base, public abstract_awaitable_base, public std::suspend_always {
        public:
            using promise_type = generator_promise<T>;

            generator(ref_counted_resource_handle coroutine_handle) noexcept : future_base(
                std::move(coroutine_handle)) {
                auto *promise = this->get_promise();
                assert(promise);
            }

            generator_promise<T> *get_promise() const {
                promise_base *base = this->get_promise_base();
                return static_cast<promise_type *>(base);
            }

            template<typename Self>
            decltype(auto) with_context(this Self &&self, execution_context *ctx) {
                self.get_promise()->set_execution_context(ctx);
                return std::forward<Self>(self);
            }

            std::optional<T> await_resume() {
                assert(this->m_coroutine_handle);
                auto *promise = get_promise();
                return promise->get_result_move();
            }

            auto by_promise(promise_base *parent_promise) {
                assert(parent_promise);

                struct awaiter {
                    generator<T> *m_generator;
                    promise_base *m_parent_promise;

                    [[nodiscard]] bool await_ready() const noexcept {
                        return m_generator->get_promise()->is_done();
                    }

                    void await_suspend(std::coroutine_handle<> h) noexcept {
                        execution_context *context = m_parent_promise->get_execution_context();
                        assert(context);

                        promise_base *promise = m_generator->get_promise();
                        if (!promise->get_execution_context()) {
                            promise->set_execution_context(context);
                        }

                        auto weak_parent = m_parent_promise->weak_borrow();

                        promise->set_parent(weak_parent);

                        auto *local_ctx = promise->get_execution_context();

                        promise->set_continuation([weak_parent = std::move(weak_parent)]() mutable {
                            auto parent = weak_parent.lock();
                            if (!parent) {
                                return;
                            }
                            auto *p = static_cast<promise_base *>(parent.get());
                            p->get_execution_context()->resume_promise_weak(p);
                        });

                        local_ctx->resume_promise_weak(promise);
                    }

                    decltype(auto) await_resume() {
                        return m_generator->get_promise()->get_result_move();
                    }
                };

                return awaiter{this, parent_promise};
            }
        };

#if __has_include(<generator>)
        template
        <typename T>
        generator<T> asyncify(std::generator<T> gen) {
            for (auto &&value: gen) {
                co_yield value;
            }
        }
#endif

        template<template<typename> typename TaskType = task, typename Fn>
        auto asyncify_function(Fn fn) {
            return [fn = std::move(fn)]<typename... Args>(
                Args &&... args) mutable -> TaskType<std::invoke_result_t<Fn, Args...>> {
                co_return co_await fn(std::forward<Args>(args)...);
            };
        }

        template<typename T>
        task<T> asyncify(std::future<T> future) {
            co_return future.get();
        }
    }


    using _details::all_of;
    using _details::any_of;
    using _details::generator;
    using _details::asyncify;
    using _details::asyncify_function;
    using _details::get_current_coroutine_context;
    using _details::get_current_promise_base;
    using _details::get_current_coroutine_info;

    template<typename T>
    using future = _details::task<T>;

    inline task<void> sleep_for(std::chrono::milliseconds duration) {
        std::this_thread::sleep_for(duration);
        co_return;
    }

    namespace _details {
        class interrupted_exception : public std::exception {
        public:
            [[nodiscard]] const char *what() const noexcept override {
                return m_message_buffer.c_str();
            }

            interrupted_exception(pin_resource_base *interrupted_by) {
                m_message_buffer = std::format("Task was interrupted by coroutine with handle: {:p}",
                                               static_cast<void *>(interrupted_by));
            }

        private:
            std::string m_message_buffer;
        };

        template<typename T>
        class COROUTINE_AWAIT_ELIDABLE interruptable_task : public task_base<T> {
        public:
            interruptable_task(ref_counted_resource_handle &&coroutine_handle) noexcept : task_base<T>(
                std::move(coroutine_handle)) {}

            interruptable_task() = default;

        public:
            template<template<typename...> typename TaskType>
            task<T> interrupt_by(
                COROUTINE_AWAIT_ELIDABLE_ARGUMENT
                this interruptable_task<T> self,
                COROUTINE_AWAIT_ELIDABLE_ARGUMENT
                TaskType<void> interrupt_signal) {
                auto *handle = interrupt_signal.get_promise();
                auto [this_task, interrupt_task] = co_await any_of(std::move(self), std::move(interrupt_signal));
                if (this_task) {
                    if (!this_task->has_value()) {
                        std::rethrow_exception(this_task->error());
                    }
                    if constexpr (!std::is_same_v<T, void>) {
                        co_return std::move(this_task->value());
                    } else {
                        co_return;
                    }
                } else {
                    throw interrupted_exception{static_cast<pin_resource_base *>(handle)};
                }
            }
        };

        inline task<void> wait_for_semaphore(std::shared_ptr<std::binary_semaphore> semaphore) {
            semaphore->acquire();
            co_return;
        }
    }

    using _details::interruptable_task;
    using _details::interrupted_exception;
    using _details::wait_for_semaphore;
}
