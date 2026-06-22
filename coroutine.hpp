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
#if __has_include(<generator>)
#include <generator>
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

        protected:
            virtual void release_resources() noexcept = 0;

            virtual void destroy() noexcept = 0;

            ~pin_resource_base() = default;

        public:
            inline void release_strong_reference() noexcept {
                uint32_t old_count = m_strong_count.fetch_sub(1, std::memory_order_release);
                if (old_count == 1) {
                    std::atomic_thread_fence(std::memory_order_acquire);
                    release_resources();
                    release_weak_reference();
                }
            }

            inline void release_weak_reference() noexcept {
                uint32_t old_count = m_weak_count.fetch_sub(1, std::memory_order_release);
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

        public:
            ref_counted_resource_handle() noexcept = default;

            ref_counted_resource_handle(pin_resource_base *resource,
                                        resource_acquisition_semantics::adopt) noexcept : m_resource(resource) {}

            ref_counted_resource_handle(pin_resource_base *resource,
                                        resource_acquisition_semantics::copy) noexcept : m_resource(resource) {
                if (m_resource) {
                    m_resource->add_strong_reference();
                }
            }

            ref_counted_resource_handle(const ref_counted_resource_handle &other) noexcept : m_resource(
                other.m_resource) {
                if (m_resource) {
                    m_resource->add_strong_reference();
                }
            }

            ref_counted_resource_handle(ref_counted_resource_handle &&other) noexcept : m_resource(
                std::exchange(other.m_resource, nullptr)) {}

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
                if (m_resource) {
                    m_resource->release_strong_reference();
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
                    m_resource->release_strong_reference();
                    m_resource = nullptr;
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
                if (m_resource) {
                    m_resource->release_weak_reference();
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
        };

        std::atomic_size_t g_promise_count{0};

        size_t debug_get_active_promise_count() noexcept {
            return g_promise_count.load(std::memory_order_relaxed);
        }

        class promise_base : public pin_resource_base {
            template<typename T>
            friend class task_base;

        protected:
            ~promise_base() {
                g_promise_count.fetch_sub(1, std::memory_order_relaxed);
            }

        public:
            promise_base() {
                g_promise_count.fetch_add(1, std::memory_order_relaxed);
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

            const std::function<void()> &get_continuation() const noexcept {
                return m_continuation;
            }

        public:
            inline void set_continuation(std::function<void()> continuation) {
                m_continuation = std::move(continuation);
            }

            inline void set_execution_context(execution_context *context) {
                m_execution_context = context;
            }

            inline void pin() {
                m_pinned_self = ref_counted_resource_handle(this, resource_acquisition_semantics::copy{});
            }

            inline ref_counted_resource_handle borrow() const noexcept {
                return ref_counted_resource_handle(const_cast<promise_base *>(this),
                                                   resource_acquisition_semantics::copy{});
            }

            inline void unpin() {
                m_pinned_self.reset();
            }

        protected:
            void release_resources() noexcept override {
                auto self = std::move(m_pinned_self);
            }

            inline void destroy() noexcept override {
                std::coroutine_handle<promise_base>::from_promise(*this).destroy();
            }

            bool cancellable() const noexcept {
                return !m_pinned_self;
            }

        protected:
            inline void on_finished() {}

            std::function<void()> m_continuation;
            ref_counted_resource_handle m_pinned_self;
            execution_context *m_execution_context = nullptr;

        public:
            execution_context *get_execution_context() const noexcept {
                return m_execution_context;
            }

            constexpr static std::suspend_always initial_suspend() noexcept {
                return {};
            }

            struct final_awaiter {
                bool await_ready() const noexcept {
                    return false;
                }

                template<typename PromiseType>
                void await_suspend(std::coroutine_handle<PromiseType> h) const noexcept {
                    auto &promise = h.promise();
                    auto continuation = std::move(promise.m_continuation);
                    promise.m_continuation = nullptr;
                    promise.m_execution_context = nullptr;

                    promise.m_pinned_self.reset();

                    if (continuation) {
                        continuation();
                    }
                }

                void await_resume() const noexcept {}
            };

            final_awaiter final_suspend() noexcept {
                return {};
            }

            ref_counted_resource_handle get_return_object() {
                return ref_counted_resource_handle(this, resource_acquisition_semantics::adopt{});
            }
        };
    }

    class execution_context {
    public:
        virtual ~execution_context() = default;

        virtual void resume_promise(_details::promise_base *promise) = 0;

        template<typename T>
        T block_on(_details::task_base<T> task);

        template<typename T>
        void execute_async(_details::task_base<T> task, auto callback);
    };


    namespace _details {
        promise_base *future_base::get_promise_base() const noexcept {
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

            promise() = default;

        public:
            using value_type = T;

            T get_result_copy() const {
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

        public:
            template<std::convertible_to<T> U>
            void return_value(U &&value) {
                m_result.template emplace<finished<T>>(std::move(value));
                on_finished();
            }

            void unhandled_exception() {
                m_result.template emplace<uncaught_exception>(std::current_exception());
                on_finished();
            }
        };

        template<>
        class promise<void> : public promise_base {
        public:
            ~promise() = default;

            promise() = default;

        public:
            using value_type = void;

            void get_result_copy() const {
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
                return get_result_copy();
            }

            void return_void() {
                m_result.template emplace<finished<void>>(std::monostate{});
                on_finished();
            }

            void unhandled_exception() {
                m_result.template emplace<uncaught_exception>(std::current_exception());
                on_finished();
            }

        private:
            std::variant<not_finished, finished<void>, uncaught_exception> m_result{not_finished{}};
        };

        template<typename T>
        class task_base : public future_base, public abstract_awaitable_base, public std::suspend_always {
        public:
            using promise_type = promise<T>;

            task_base() = default;

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

            task_base<T> &by_promise(promise_base *parent_promise) {
                assert(parent_promise);


                if (!this->m_coroutine_handle) {
                    throw std::runtime_error("No coroutine associated with this task.");
                }

                execution_context *context = parent_promise->get_execution_context();
                assert(context);
                promise_base *promise = this->get_promise();
                if (!promise->get_execution_context()) {
                    // Inherit parent's execution context if not set yet
                    promise->set_execution_context(context);
                }

                auto weak_parent_handle = ref_counted_resource_weak_handle(
                    parent_promise, resource_acquisition_semantics::copy{});

                assert(!promise->m_continuation); // Ensure no continuation is set yet

                promise->set_continuation([weak_parent_handle = std::move(weak_parent_handle), context]() mutable {
                    if (auto parent_handle = weak_parent_handle.lock()) {
                        context->resume_promise(static_cast<promise_base *>(parent_handle.get()));
                    }
                });

                promise->get_execution_context()->resume_promise(promise);

                return *this;
            }

            T await_resume() {
                if (!this->m_coroutine_handle) {
                    throw std::runtime_error("No coroutine associated with this task.");
                }
                promise<T> *promise = get_promise();
                return promise->get_result_move();
            }

            task_base with_context(execution_context *ctx) && {
                this->get_promise()->set_execution_context(ctx);
                return std::move(*this);
            }

            const ref_counted_resource_handle &get_coroutine_handle() const noexcept {
                return this->m_coroutine_handle;
            }

            ref_counted_resource_handle &get_coroutine_handle() noexcept {
                return this->m_coroutine_handle;
            }
        };

        template<typename T>
        class task;

        template<typename T>
        class cancelable_task : public task_base<T> {
        public:
            using typename task_base<T>::promise_type;

            using task_base<T>::task_base;

            void cancel() {
                this->m_coroutine_handle.reset();
            }

            cancelable_task() = default;

            operator task<T>() && {
                return task<T>(std::move(this->m_coroutine_handle));
            }

            task<T> into() && {
                return task<T>(std::move(this->m_coroutine_handle));
            }
        };

        template<typename T>
        class task : public task_base<T> {
        public:
            using typename task_base<T>::promise_type;

            task(ref_counted_resource_handle coroutine_handle) noexcept : task_base<T>(std::move(coroutine_handle)) {
                auto *promise = this->get_promise();
                assert(promise);
                promise->pin();
            }

            task() = default;
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

        get_context_type get_current_coroutine_context() {
            return {};
        }

        namespace _impl_thread_pool {
            class IThreadPool {
            public:
                virtual ~IThreadPool() = default;

            public:
                virtual void EnqueueFunc(std::move_only_function<void()> task) = 0;

            public:
                template<typename Fn, typename... Args> requires std::invocable<Fn, Args...>
                void EnqueueDetached(Fn &&fn, Args &&... args) {
                    std::move_only_function<void()> task = [tup = std::make_tuple(
                                    std::forward<Fn>(fn), std::forward<Args>(args)...)
                            ]() mutable {
                        std::apply([]<typename... Tps>(const auto &first, Tps &&... rest) {
                            first(std::forward<Tps>(rest)...);
                        }, std::move(tup));
                    };
                    EnqueueFunc(std::move(task));
                }

                template<typename Fn, typename... Args> requires std::invocable<Fn, Args...>
                auto Enqueue(Fn &&fn, Args &&... args) {
                    using ReturnType = std::invoke_result_t<Fn, Args...>;

                    std::promise<ReturnType> promise;
                    std::future<ReturnType> future = promise.get_future();
                    auto task = std::move_only_function<void()>(
                        [promise = std::move(promise), tup = std::make_tuple(
                            std::forward<Fn>(fn), std::forward<Args>(args)...)]() mutable {
                            try {
                                if constexpr (std::is_same_v<ReturnType, void>) {
                                    std::apply([]<typename... Tps>(auto &&first, Tps &&... rest) {
                                        std::forward<decltype(first)>(first)(std::forward<Tps>(rest)...);
                                    }, std::move(tup));
                                    promise.set_value();
                                } else {
                                    auto result = std::apply([]<typename... Tps>(auto &&first, Tps &&... rest) {
                                        return std::forward<decltype(first)>(first)(std::forward<Tps>(rest)...);
                                    }, std::move(tup));
                                    promise.set_value(std::move(result));
                                }
                            } catch (...) {
                                try {
                                    promise.set_exception(std::current_exception());
                                } catch (...) {
                                    // set_exception may throw too
                                }
                            }
                        });

                    EnqueueFunc([task = std::move(task)] mutable { std::move(task)(); });
                    return std::move(future);
                }

            public:
                virtual void Join() = 0;
            };

            class SharedThreadPool : public IThreadPool {
                friend class std::shared_ptr<SharedThreadPool>;

            public:
                template<typename OnStartUp, typename OnShutDown>
                static std::shared_ptr<IThreadPool> Create(size_t size = std::jthread::hardware_concurrency() * 2,
                                                           OnStartUp &&onStartUp = {},
                                                           OnShutDown &&onShutDown = {}) {
                    // ReSharper disable once CppSmartPointerVsMakeFunction
                    auto pool = std::shared_ptr<SharedThreadPool>(new SharedThreadPool(size));
                    pool->SetSelf(pool);
                    Start(pool, std::forward<OnStartUp>(onStartUp), std::forward<OnShutDown>(onShutDown));
                    return pool;
                }

            protected:
                SharedThreadPool(size_t size = std::jthread::hardware_concurrency() * 2) {
                    m_WorkerThreads.reserve(size);
                }

            public:
                void Join() override {
                    WaitAllTaskToFinish();
                    m_ShouldStop = true;
                    m_Condition.notify_all();
                    for (auto &thread: m_WorkerThreads) {
                        if (thread.joinable()) {
                            thread.join();
                        }
                    }
                    m_WorkerThreads.clear();
                }

                void SetSelf(const std::shared_ptr<SharedThreadPool> &self) {
                    m_Self = self;
                }

                template<typename OnStartUp, typename OnShutDown>
                static std::shared_ptr<IThreadPool> Start(std::shared_ptr<SharedThreadPool> self,
                                                          OnStartUp &&onStartUp = {},
                                                          OnShutDown &&onShutDown = {}) {
                    for (size_t i = 0; i < self->m_WorkerThreads.capacity(); ++i) {
                        self->m_WorkerThreads.emplace_back([self = self->m_Self.lock(), onStartUp, onShutDown] {
                            if constexpr (std::is_invocable_v<OnStartUp>) {
                                onStartUp();
                            }
                            while (!(self->m_ShouldStop && self->m_Tasks.empty())) {
                                std::move_only_function<void()> task;

                                // in a scope
                                {
                                    std::unique_lock lock(self->m_Mutex);
                                    self->m_Condition.wait_for(lock, std::chrono::milliseconds(100), [&self] {
                                        return self->m_ShouldStop || !self->m_Tasks.empty();
                                    });

                                    if (self->m_ShouldStop && self->m_Tasks.empty()) {
                                        return;
                                    }

                                    if (self->m_Tasks.empty()) {
                                        continue;
                                    }

                                    task = std::move(self->m_Tasks.front());
                                    self->m_Tasks.pop();
                                }

                                try {
                                    task();
                                } catch (std::exception &e) {
                                    std::cerr << "Exception in thread pool task: " << e.what() << std::endl;
                                } catch (...) {
                                    std::cerr << "Unknown exception in thread pool task" << std::endl;
                                }
                            }
                            if constexpr (std::is_invocable_v<OnShutDown>) {
                                onShutDown();
                            }
                        });
                    }
                    return self;
                }

                void EnqueueFunc(std::move_only_function<void()> task) override {
                    std::lock_guard lock(m_Mutex);
                    m_Tasks.emplace(std::move(task));
                    m_Condition.notify_one();
                }

                ~SharedThreadPool() {
                    if (m_ShouldStop) {
                        m_WorkerThreads.clear();
                        return;
                    }

                    m_ShouldStop = true;
                    m_Condition.notify_all();
                    for (auto &thread: m_WorkerThreads) {
                        if (thread.joinable()) {
                            thread.join();
                        }
                    }
                }

                void DetachAll() {
                    for (auto &thread: m_WorkerThreads) {
                        if (thread.joinable()) {
                            thread.detach();
                        }
                    }
                    m_WorkerThreads.clear();
                }

                void WaitAllTaskToFinish() {
                    while (true) {
                        {
                            std::lock_guard lock(m_Mutex);
                            if (m_Tasks.empty()) {
                                break;
                            }
                        }
                        std::this_thread::yield();
                    }
                }

            private:
                std::vector<std::jthread> m_WorkerThreads{};
                std::condition_variable m_Condition{};
                std::mutex m_Mutex{};
                std::queue<std::move_only_function<void()>> m_Tasks{};
                std::atomic_bool m_ShouldStop{false};
                std::weak_ptr<SharedThreadPool> m_Self;
            };
        }

        class multithreaded_execution_context : public execution_context {
        public:
            explicit multithreaded_execution_context(uint32_t thread_count = std::jthread::hardware_concurrency() * 2) {
                m_thread_pool = _impl_thread_pool::SharedThreadPool::Create(thread_count, nullptr, nullptr);
            }

            void resume_promise(promise_base *promise) override {
                auto promise_handle = promise->borrow();
                m_thread_pool->EnqueueFunc([promise_handle]mutable {
                    auto *promise = static_cast<promise_base *>(promise_handle.get());
                    auto handle = std::coroutine_handle<promise_base>::from_promise(*promise);
                    handle.resume();
                    promise_handle.reset();
                });
            }

            ~multithreaded_execution_context() override {
                m_thread_pool->Join();
            }

        private:
            std::shared_ptr<_impl_thread_pool::IThreadPool> m_thread_pool;
        };
    }

    template<typename T>
    T execution_context::block_on(_details::task_base<T> task) {
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

        resume_promise(promise);

        std::unique_lock lock(mtx);
        cv.wait(lock, [&] { return finished; });

        if constexpr (std::is_same_v<T, void>) {
            task.get_promise()->get_result_move();
        } else {
            return task.get_promise()->get_result_move();
        }
    }

    template<typename T>
    void execution_context::execute_async(_details::task_base<T> task, auto callback) {
        std::function<void()> continuation = [callback = std::forward<decltype(callback)>(callback), promise = task.
                    get_promise()]() mutable {
            try {
                if (promise) {
                    if constexpr (std::is_same_v<T, void>) {
                        promise->get_result_copy();
                        callback();
                    } else {
                        auto result = promise->get_result_move();
                        callback(std::move(result));
                    }
                }
            } catch (const std::exception &e) {
                std::cerr << "Exception in async task: " << e.what() << std::endl;
            } catch (...) {
                std::cerr << "Unknown exception in async task" << std::endl;
            }
        };

        auto *promise = task.get_promise();
        assert(promise);
        promise->set_continuation(std::move(continuation));
        promise->set_execution_context(this);

        resume_promise(promise);
    }

    template<typename T>
    using task = _details::task<T>;

    template<typename T>
    using cancelable_task = _details::cancelable_task<T>;

    namespace _details {
        template<template<typename> typename TaskType, typename T>
        TaskType<std::expected<T, std::exception_ptr>> into_non_throw(TaskType<T> task) {
            try {
                if constexpr (std::is_same_v<T, void>) {
                    co_await std::move(task);
                    co_return std::expected<void, std::exception_ptr>{};
                } else {
                    T result = co_await std::move(task);
                    co_return std::expected<T, std::exception_ptr>{std::move(result)};
                }
            } catch (...) {
                co_return std::unexpected{std::current_exception()};
            }
        }

        template<template<typename> typename TaskType = task, typename... T>
        TaskType<std::tuple<std::expected<T, std::exception_ptr>...>> all_of(TaskType<T>... tasks) {
            struct SharedState {
                std::atomic_size_t remaining{sizeof...(T)};
                std::coroutine_handle<> parent_coro;
                std::tuple<TaskType<std::expected<T, std::exception_ptr>>...> non_throw_tasks;
            };

            auto state = std::make_shared<SharedState>();
            state->non_throw_tasks = std::make_tuple(into_non_throw(std::move(tasks))...);

            auto *context = co_await get_current_coroutine_context();

            auto continuation = [state]() mutable {
                if (state->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    if (state->parent_coro) {
                        state->parent_coro.resume();
                    }
                }
            };

            struct awaiter {
                std::shared_ptr<SharedState> state;
                execution_context *ctx;
                decltype(continuation) cont;

                bool await_ready() const noexcept {
                    return sizeof...(T) == 0;
                }

                void await_suspend(std::coroutine_handle<> h) noexcept {
                    state->parent_coro = h;
                    std::apply([&](auto &&... t) {
                        (t.get_promise()->set_continuation(cont), ...);
                        (t.get_promise()->set_execution_context(ctx), ...);
                        (ctx->resume_promise(t.get_promise()), ...);
                    }, state->non_throw_tasks);
                }

                void await_resume() const noexcept {}
            };

            co_await awaiter{state, context, continuation};

            co_return std::apply([](auto &&... task) {
                return std::make_tuple(task.get_promise()->get_result_move()...);
            }, state->non_throw_tasks);
        }

        template<template<typename> typename TaskType = task, typename T>
        TaskType<std::vector<std::expected<T, std::exception_ptr>>> all_of(std::vector<TaskType<T>> tasks) {
            struct SharedState {
                std::atomic_size_t remaining;
                std::coroutine_handle<> parent_coro;
                std::vector<TaskType<std::expected<T, std::exception_ptr>>> non_throw_tasks;

                explicit SharedState(size_t size) : remaining(size) {}
            };

            auto state = std::make_shared<SharedState>(tasks.size());
            auto *context = co_await get_current_coroutine_context();

            state->non_throw_tasks.reserve(tasks.size());
            for (auto &t: tasks) {
                state->non_throw_tasks.push_back(into_non_throw<TaskType, T>(std::move(t)));
            }

            auto continuation = [state]() mutable {
                if (state->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    if (state->parent_coro) {
                        state->parent_coro.resume();
                    }
                }
            };

            struct awaiter {
                std::shared_ptr<SharedState> state;
                execution_context *ctx;
                decltype(continuation) cont;

                bool await_ready() const noexcept {
                    return state->non_throw_tasks.empty();
                }

                void await_suspend(std::coroutine_handle<> h) noexcept {
                    state->parent_coro = h;
                    for (auto &t: state->non_throw_tasks) {
                        t.get_promise()->set_continuation(cont);
                        t.get_promise()->set_execution_context(ctx);
                        ctx->resume_promise(t.get_promise());
                    }
                }

                void await_resume() const noexcept {}
            };

            co_await awaiter{state, context, continuation};

            std::vector<std::expected<T, std::exception_ptr>> results;
            results.reserve(state->non_throw_tasks.size());
            for (auto &t: state->non_throw_tasks) {
                results.push_back(t.get_promise()->get_result_move());
            }
            co_return results;
        }

        template<template<typename> typename TaskType = task, typename... T>
        TaskType<std::tuple<std::optional<std::expected<T, std::exception_ptr>>...>> any_of(TaskType<T>... tasks) {
            struct SharedState {
                std::atomic_bool ready{false};
                std::atomic<int> dispatch_flags{0};
                std::coroutine_handle<> parent_coro;
                std::tuple<std::optional<std::expected<T, std::exception_ptr>>...> result;
                std::tuple<TaskType<T>...> kept_tasks;
            };

            auto state = std::make_shared<SharedState>();
            state->kept_tasks = std::make_tuple(std::move(tasks)...);
            auto *context = co_await get_current_coroutine_context();

            struct awaiter {
                std::shared_ptr<SharedState> state;
                execution_context *ctx;

                bool await_ready() const noexcept {
                    return sizeof...(T) == 0;
                }

                void await_suspend(std::coroutine_handle<> h) noexcept {
                    state->parent_coro = h;

                    auto setup_task = [&]<size_t I>(auto &task, std::integral_constant<size_t, I>) {
                        if (!task.has_value()) {
                            return;
                        }

                        auto *promise = task.get_promise();
                        if (!promise) {
                            return;
                        }

                        promise->set_execution_context(ctx);

                        promise->set_continuation([state = this->state, promise]() mutable {
                            if (!state->ready.exchange(true, std::memory_order_acq_rel)) {
                                using CurrentT = std::tuple_element_t<I, std::tuple<T...>>;
                                try {
                                    if constexpr (std::is_same_v<CurrentT, void>) {
                                        promise->get_result_move();
                                        std::get<I>(state->result) = std::expected<void, std::exception_ptr>{};
                                    } else {
                                        std::get<I>(state->result) = std::expected<CurrentT, std::exception_ptr>{
                                            promise->get_result_move()
                                        };
                                    }
                                } catch (...) {
                                    std::get<I>(state->result) = std::unexpected{std::current_exception()};
                                }

                                int old_flags = state->dispatch_flags.fetch_or(1, std::memory_order_acq_rel);
                                if ((old_flags & 2) != 0) {
                                    if (state->parent_coro) {
                                        state->parent_coro.resume();
                                    }
                                }
                            }
                        });

                        ctx->resume_promise(promise);
                    };

                    std::apply([&](auto &... t) {
                        [&]<size_t... Is>(std::index_sequence<Is...>) {
                            (setup_task(t, std::integral_constant<size_t, Is>{}), ...);
                        }(std::index_sequence_for<T...>{});
                    }, state->kept_tasks);

                    int old_flags = state->dispatch_flags.fetch_or(2, std::memory_order_acq_rel);
                    if ((old_flags & 1) != 0) {
                        if (state->parent_coro) {
                            state->parent_coro.resume();
                        }
                    }
                }

                void await_resume() const noexcept {}
            };

            co_await awaiter{state, context};

            std::tuple<std::optional<std::expected<T, std::exception_ptr>>...> ret_val = std::move(state->result);

            std::apply([](auto &... t) {
                (t.reset(), ...);
            }, state->kept_tasks);

            co_return ret_val;
        }

        template<template<typename> typename TaskType = task, typename T>
        TaskType<std::optional<std::expected<T, std::exception_ptr>>> any_of(std::vector<TaskType<T>> tasks) {
            struct SharedState {
                std::atomic_bool ready{false};
                std::atomic<int> dispatch_flags{0};
                std::coroutine_handle<> parent_coro;
                std::optional<std::expected<T, std::exception_ptr>> result;
                std::vector<TaskType<T>> kept_tasks;
            };

            auto state = std::make_shared<SharedState>();
            state->kept_tasks = std::move(tasks);
            auto *context = co_await get_current_coroutine_context();

            struct awaiter {
                std::shared_ptr<SharedState> state;
                execution_context *ctx;

                bool await_ready() const noexcept {
                    return state->kept_tasks.empty();
                }

                void await_suspend(std::coroutine_handle<> h) noexcept {
                    state->parent_coro = h;

                    for (auto &task: state->kept_tasks) {
                        if (!task.has_value()) {
                            continue;
                        }

                        auto *promise = task.get_promise();
                        if (!promise) {
                            continue;
                        }

                        promise->set_execution_context(ctx);

                        promise->set_continuation([state = this->state, promise]() mutable {
                            if (!state->ready.exchange(true, std::memory_order_acq_rel)) {
                                try {
                                    if constexpr (std::is_same_v<T, void>) {
                                        promise->get_result_move();
                                        state->result = std::expected<void, std::exception_ptr>{};
                                    } else {
                                        state->result = std::expected<T, std::exception_ptr>{
                                            promise->get_result_move()
                                        };
                                    }
                                } catch (...) {
                                    state->result = std::unexpected{std::current_exception()};
                                }

                                int old_flags = state->dispatch_flags.fetch_or(1, std::memory_order_acq_rel);
                                if ((old_flags & 2) != 0) {
                                    if (state->parent_coro) {
                                        state->parent_coro.resume();
                                    }
                                }
                            }
                        });

                        ctx->resume_promise(promise);
                    }

                    int old_flags = state->dispatch_flags.fetch_or(2, std::memory_order_acq_rel);
                    if ((old_flags & 1) != 0) {
                        if (state->parent_coro) {
                            state->parent_coro.resume();
                        }
                    }
                }

                void await_resume() const noexcept {}
            };

            co_await awaiter{state, context};

            std::optional<std::expected<T, std::exception_ptr>> ret_val = std::move(state->result);

            for (auto &t: state->kept_tasks) {
                t.reset();
            }

            co_return ret_val;
        }

        template<typename T> requires (!std::same_as<T, void>)
        class generator_promise : public promise_base {
        public:
            std::optional<T> get_resource_move() {
                if (std::coroutine_handle<generator_promise> promise = std::coroutine_handle<
                    generator_promise>::from_promise(*this); promise.done()) {
                    return std::nullopt;
                }

                switch (m_current_value.index()) {
                    case 0: {
                        auto value = std::move(std::get<0>(m_current_value));
                        m_current_value.template emplace<std::monostate>();
                        return value;
                    }
                    case 1:
                        std::rethrow_exception(std::get<std::exception_ptr>(m_current_value));
                    default:
                        throw std::runtime_error("No value yielded.");
                }
            }

            generator_promise with_context(execution_context *ctx) && {
                this->set_execution_context(ctx);
                return std::move(*this);
            }

            void return_void() {
                m_current_value.template emplace<std::monostate>();
                on_finished();
            }

            void unhandled_exception() {
                m_current_value.template emplace<std::exception_ptr>(std::current_exception());
                on_finished();
            }

            template<std::convertible_to<T> U>
            std::suspend_always yield_value(U &&value) {
                m_current_value.template emplace<T>(std::forward<U>(value));
                on_finished();
                return {};
            }

            bool is_done() noexcept {
                return std::coroutine_handle<generator_promise>::from_promise(*this).done();
            }

        private:
            std::variant<T, std::exception_ptr, std::monostate> m_current_value{std::monostate{}};
        };

        template<typename T> requires (!std::same_as<T, void>)
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

            void with_context(execution_context *ctx) && {
                this->get_promise()->set_execution_context(ctx);
            }

            std::optional<T> await_resume() {
                assert(this->m_coroutine_handle);
                auto *promise = get_promise();
                return promise->get_resource_move();
            }

            generator &by_promise(promise_base *parent_promise) {
                assert(parent_promise);
                generator_promise<T> *promise = this->get_promise();
                if (!promise->get_execution_context()) {
                    promise->set_execution_context(parent_promise->get_execution_context());
                }

                auto weak_parent_handle = ref_counted_resource_weak_handle(
                    parent_promise, resource_acquisition_semantics::copy{});

                assert(!promise->get_continuation()); // Ensure no continuation is set, or is consumed
                promise->set_continuation([weak_parent_handle = std::move(weak_parent_handle)]() mutable {
                    if (auto parent_handle = weak_parent_handle.lock()) {
                        std::coroutine_handle<promise_base>::from_promise(
                            *static_cast<promise_base *>(parent_handle.get())).resume();
                    }
                });

                promise->get_execution_context()->resume_promise(promise);

                return *this;
            }
        };

#if __has_include(<generator>)
        template<typename T>
        generator<T> asyncify(std::generator<T> gen) {
            for (auto &&value: gen) {
                co_yield value;
            }
        }
#endif

        template<template<typename >typename TaskType = task, typename Fn>
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

    template<typename T>
    using future = _details::task<T>;

    task<void> sleep_for(std::chrono::milliseconds duration) {
        std::this_thread::sleep_for(duration);
        co_return;
    }
}
