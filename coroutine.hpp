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

#ifdef __clang__
#define COROUTINE_AWAIT_ELIDABLE [[clang::coro_await_elidable]]
#define COROUTINE_AWAIT_ELIDABLE_ARGUMENT [[clang::coro_await_elidable_argument]]
#else
#define COROUTINE_AWAIT_ELIDABLE
#define COROUTINE_AWAIT_ELIDABLE_ARGUMENT
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

            ref_counted_resource_handle release_handle() noexcept {
                return std::move(m_coroutine_handle);
            }
        };

        std::atomic_size_t g_promise_count{0};

        size_t debug_get_active_promise_count() noexcept {
            return g_promise_count.load(std::memory_order_acquire);
        }

        class promise_base : public pin_resource_base {
            template<typename T>
            friend class task_base;

        protected:
            ~promise_base() {
                g_promise_count.fetch_sub(1, std::memory_order_release);
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
                auto self = std::move(m_pinned_self);
            }

            inline void destroy() noexcept override {
                std::coroutine_handle<promise_base>::from_promise(*this).destroy();
            }

        protected:
            inline void on_finished() {}

            std::function<void()> m_continuation;
            ref_counted_resource_handle m_pinned_self;
            execution_context *m_execution_context = nullptr;
            bool cancelable = false;

        public:
            inline bool is_cancelable() const noexcept {
                return cancelable;
            }

            inline void set_cancelable(bool value) noexcept {
                cancelable = value;
            }

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

        virtual void resume_promise_strong(_details::promise_base *promise) = 0;

        virtual void resume_promise_detached(_details::future_base &&future) = 0;

        virtual void resume_promise_weak(_details::promise_base *promise) = 0;

        template<typename T>
        T block_on(_details::task_base<T> task);

        virtual void wait_idle() = 0;
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

        struct task_base_flag {};

        template<typename T>
        class COROUTINE_AWAIT_ELIDABLE task_base : public future_base, public abstract_awaitable_base,
                                                   public std::suspend_always, public task_base_flag {
        public:
            using promise_type = promise<T>;

            task_base() = default;

            task_base(ref_counted_resource_handle coroutine_handle) noexcept : future_base(
                std::move(coroutine_handle)) {}

            task_base into_pure_rvalue() && {
                return {std::move(this->m_coroutine_handle)};
            }

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
                    task_base<T> m_task;
                    promise_base *m_parent_promise;

                    bool await_ready() const noexcept {
                        return false;
                    }

                    void await_suspend(std::coroutine_handle<> h) noexcept {
                        execution_context *context = m_parent_promise->get_execution_context();
                        assert(context);

                        auto *promise = m_task.get_promise();
                        if (!promise->get_execution_context()) {
                            promise->set_execution_context(context);
                        }

                        auto weak_parent = ref_counted_resource_weak_handle(
                            m_parent_promise, resource_acquisition_semantics::copy{});

                        auto *local_ctx = promise->get_execution_context();

                        promise->set_continuation([weak_parent = std::move(weak_parent)]() mutable {
                            auto parent = weak_parent.lock();
                            if (!parent) {
                                return;
                            }
                            auto *p = static_cast<promise_base *>(parent.get());
                            p->get_execution_context()->resume_promise_strong(p);
                        });

                        local_ctx->resume_promise_strong(promise);
                    }

                    decltype(auto) await_resume() {
                        return m_task.get_promise()->get_result_move();
                    }
                };

                return awaiter{std::move(*this), parent_promise};
            }

            T await_resume() {
                if (!this->m_coroutine_handle) {
                    throw std::runtime_error("No coroutine associated with this task.");
                }
                promise<T> *promise = get_promise();
                return promise->get_result_move();
            }

            template<typename Self> requires (std::is_rvalue_reference_v<Self &&> && !std::is_const_v<
                                                  std::remove_reference_t<Self>>)
            auto with_context(this Self &&self, execution_context *ctx) {
                self.get_promise()->set_execution_context(ctx);
                return std::move(self);
            }

            const ref_counted_resource_handle &get_coroutine_handle() const noexcept {
                return this->m_coroutine_handle;
            }

            ref_counted_resource_handle &get_coroutine_handle() noexcept {
                return this->m_coroutine_handle;
            }

            ref_counted_resource_handle release_handle() && noexcept {
                return std::move(this->m_coroutine_handle);
            }
        };

        template<typename T>
        class task;

        template<typename T>
        class COROUTINE_AWAIT_ELIDABLE cancelable_task : public task_base<T> {
        public:
            using typename task_base<T>::promise_type;

            cancelable_task(ref_counted_resource_handle coroutine_handle) noexcept : task_base<T>(
                std::move(coroutine_handle)) {
                auto *promise = this->get_promise();
                assert(promise);
                promise->set_cancelable(true);
            }

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
        class COROUTINE_AWAIT_ELIDABLE task : public task_base<T> {
        public:
            using typename task_base<T>::promise_type;

            task(ref_counted_resource_handle coroutine_handle) noexcept : task_base<T>(std::move(coroutine_handle)) {}

            task() = default;

            template<std::derived_from<task_base<T>>>
            task(task_base<T> base) noexcept : task<T>(std::move(base).release_handle()) {}
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

            void resume_promise_strong(promise_base *promise) override {
                if (!promise->is_cancelable()) {
                    promise->pin();
                }

                auto promise_handle = promise->borrow();
                m_thread_pool->EnqueueFunc([promise_handle]mutable {
                    auto *promise = static_cast<promise_base *>(promise_handle.get());
                    auto handle = std::coroutine_handle<promise_base>::from_promise(*promise);

                    if (handle && !handle.done()) {
                        handle.resume();
                    } else {
                        // __debugbreak();
                    }

                    promise_handle.reset();
                });
            }

            void resume_promise_detached(future_base &&future) override {
                auto promise_handle = std::move(future.release_handle());
                m_thread_pool->EnqueueFunc([promise_handle = std::move(promise_handle)] mutable {
                    auto *promise = static_cast<promise_base *>(promise_handle.get());
                    auto handle = std::coroutine_handle<promise_base>::from_promise(*promise);

                    if (handle && !handle.done()) {
                        handle.resume();
                    } else {
                        // __debugbreak();
                    }
                });
            }

            void resume_promise_weak(_details::promise_base *promise) override {
                auto weak_handle = promise->weak_borrow();
                m_thread_pool->EnqueueFunc([weak_handle = std::move(weak_handle)] mutable {
                    auto locked = weak_handle.lock();
                    if (!locked) {
                        return;
                    }
                    auto *promise = static_cast<promise_base *>(locked.get());
                    auto handle = std::coroutine_handle<promise_base>::from_promise(*promise);

                    if (handle && !handle.done()) {
                        handle.resume();
                    } else {
                        // __debugbreak();
                    }
                });
            }


            ~multithreaded_execution_context() override {
                m_thread_pool->Join();
            }


            void wait_idle() override {
                // m_thread_pool->Join();
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

        resume_promise_strong(promise);

        std::unique_lock lock(mtx);
        cv.wait(lock, [&] { return finished; });


        if constexpr (std::is_same_v<T, void>) {
            task.get_promise()->get_result_move();
        } else {
            return task.get_promise()->get_result_move();
        }

    }

    // template<typename T>
    // void execution_context::execute_async(_details::task_base<T> task, auto callback) {
    //     std::function<void()> continuation = [callback = std::forward<decltype(callback)>(callback), promise = task.
    //                 get_promise()]() mutable {
    //         try {
    //             if (promise) {
    //                 if constexpr (std::is_same_v<T, void>) {
    //                     promise->get_result_copy();
    //                     callback();
    //                 } else {
    //                     auto result = promise->get_result_move();
    //                     callback(std::move(result));
    //                 }
    //             }
    //         } catch (const std::exception &e) {
    //             std::cerr << "Exception in async task: " << e.what() << std::endl;
    //         } catch (...) {
    //             std::cerr << "Unknown exception in async task" << std::endl;
    //         }
    //     };
    //
    //     auto *promise = task.get_promise();
    //     assert(promise);
    //     promise->set_continuation(std::move(continuation));
    //     promise->set_execution_context(this);
    //
    //     resume_promise_strong(promise);
    // }

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


        template<template<typename> typename TaskType, typename T>
        TaskType<std::expected<T, std::exception_ptr>> into_non_throw(
            COROUTINE_AWAIT_ELIDABLE_ARGUMENT TaskType<T> task) {
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

        // template<template<typename> typename TaskType = task, typename... T>
        // TaskType<std::tuple<std::expected<T, std::exception_ptr>...>> all_of(
        //     COROUTINE_AWAIT_ELIDABLE_ARGUMENT TaskType<T>... tasks) {
        //     struct State {
        //         std::atomic_size_t remaining{sizeof...(T)};
        //         promise_base *parent_promise = nullptr;
        //         std::tuple<TaskType<std::expected<T, std::exception_ptr>>...> non_throw_tasks;
        //     };
        //
        //     State state;
        //     state.non_throw_tasks = std::make_tuple(into_non_throw(std::move(tasks))...);
        //
        //     auto info = co_await get_current_coroutine_info();
        //     auto weak_self = info.self->weak_borrow();
        //
        //     struct awaiter {
        //         State *state;
        //         ref_counted_resource_weak_handle weak_self;
        //         execution_context *ctx;
        //
        //         bool await_ready() const noexcept { return sizeof...(T) == 0; }
        //
        //         void await_suspend(std::coroutine_handle<> h) noexcept {
        //             auto *parent_promise = &std::coroutine_handle<promise_base>::from_address(h.address()).promise();
        //             state->parent_promise = parent_promise;
        //             auto ref = parent_promise->borrow(); // Ensure the parent promise stays alive while we're waiting
        //             State *local_state = state;
        //             auto local_weak = weak_self;
        //             auto *local_ctx = ctx;
        //
        //             auto setup_task = [&]<size_t I>(auto &task, std::integral_constant<size_t, I>) {
        //                 auto *promise = task.get_promise();
        //                 promise->set_execution_context(local_ctx);
        //
        //                 promise->set_continuation([local_weak, local_state, local_ctx]() mutable {
        //                     auto locked = local_weak.lock();
        //                     if (!locked) return;
        //
        //                     if (local_state->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        //                         if (local_state->parent_promise) {
        //                             local_ctx->resume_promise_strong(local_state->parent_promise);
        //                         }
        //                     }
        //                 });
        //                 local_ctx->resume_promise_strong(promise);
        //             };
        //
        //
        //             std::apply([&](auto &... t) {
        //                 [&]<size_t... Is>(std::index_sequence<Is...>) {
        //                     (setup_task(t, std::integral_constant<size_t, Is>{}), ...);
        //                 }(std::index_sequence_for<T...>{});
        //             }, local_state->non_throw_tasks);
        //
        //
        //         }
        //
        //         void await_resume() const noexcept {}
        //     };
        //
        //     co_await awaiter{&state, weak_self, info.context};
        //
        //     std::tuple<std::expected<T, std::exception_ptr>...> ret_val = std::apply([](auto &&... t) {
        //         return std::make_tuple(t.get_promise()->get_result_move()...);
        //     }, state.non_throw_tasks);
        //
        //     co_return ret_val;
        // }
        //
        // template<template<typename> typename TaskType = task, typename T>
        // TaskType<std::vector<std::expected<T, std::exception_ptr>>> all_of(std::vector<TaskType<T>> tasks) {
        //     struct State {
        //         std::atomic_size_t remaining;
        //         promise_base *parent_promise = nullptr;
        //         std::vector<TaskType<std::expected<T, std::exception_ptr>>> non_throw_tasks;
        //         explicit State(size_t size) : remaining(size) {}
        //     };
        //
        //     State state(tasks.size());
        //     state.non_throw_tasks.reserve(tasks.size());
        //     for (auto &t: tasks) state.non_throw_tasks.push_back(into_non_throw<TaskType, T>(std::move(t)));
        //
        //     auto info = co_await get_current_coroutine_info();
        //     auto weak_self = info.self->weak_borrow();
        //
        //     struct awaiter {
        //         State *state;
        //         ref_counted_resource_weak_handle weak_self;
        //         execution_context *ctx;
        //
        //         bool await_ready() const noexcept { return state->non_throw_tasks.empty(); }
        //
        //         void await_suspend(std::coroutine_handle<> h) noexcept {
        //             auto *parent_promise = &std::coroutine_handle<promise_base>::from_address(h.address()).promise();
        //             state->parent_promise = parent_promise;
        //             auto ref = parent_promise->borrow();
        //
        //             State *local_state = state;
        //             auto local_weak = weak_self;
        //             auto *local_ctx = ctx;
        //
        //             for (auto &t: local_state->non_throw_tasks) {
        //                 auto *promise = t.get_promise();
        //                 promise->set_execution_context(local_ctx);
        //
        //                 promise->set_continuation([local_weak, local_state, local_ctx]() mutable {
        //                     auto locked = local_weak.lock();
        //                     if (!locked) return;
        //
        //                     if (local_state->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        //                         if (local_state->parent_promise) {
        //                             local_ctx->resume_promise_strong(local_state->parent_promise);
        //                         }
        //                     }
        //                 });
        //                 local_ctx->resume_promise_strong(promise);
        //             }
        //         }
        //
        //         void await_resume() const noexcept {}
        //     };
        //
        //     co_await awaiter{&state, weak_self, info.context};
        //
        //     std::vector<std::expected<T, std::exception_ptr>> results;
        //     results.reserve(state.non_throw_tasks.size());
        //     for (auto &t: state.non_throw_tasks) results.push_back(t.get_promise()->get_result_move());
        //
        //     co_return results;
        // }
        //
        // template<template<typename> typename TaskType = task, typename... T>
        // TaskType<std::tuple<std::optional<std::expected<T, std::exception_ptr>>...>> any_of(
        //     COROUTINE_AWAIT_ELIDABLE_ARGUMENT TaskType<T>... tasks) {
        //     struct State {
        //         std::atomic_bool ready{false};
        //         promise_base *parent_promise = nullptr;
        //         std::tuple<std::optional<std::expected<T, std::exception_ptr>>...> result;
        //         std::tuple<TaskType<T>...> kept_tasks;
        //     };
        //
        //     State state;
        //     state.kept_tasks = std::make_tuple(std::move(tasks)...);
        //
        //     auto info = co_await get_current_coroutine_info();
        //     auto weak_self = info.self->weak_borrow();
        //
        //     struct awaiter {
        //         State *state;
        //         ref_counted_resource_weak_handle weak_self;
        //         execution_context *ctx;
        //
        //         bool await_ready() const noexcept { return sizeof...(T) == 0; }
        //
        //         void await_suspend(std::coroutine_handle<> h) noexcept {
        //             auto *parent_promise = &std::coroutine_handle<promise_base>::from_address(h.address()).promise();
        //             state->parent_promise = parent_promise;
        //             auto ref = parent_promise->borrow();
        //
        //             State *local_state = state;
        //             auto local_weak = weak_self;
        //             auto *local_ctx = ctx;
        //
        //             auto setup_task = [&]<size_t I>(auto &task, std::integral_constant<size_t, I>) {
        //                 auto *promise = task.get_promise();
        //                 promise->set_execution_context(local_ctx);
        //
        //                 promise->set_continuation([local_weak, local_state, local_ctx, promise]() mutable {
        //                     auto locked = local_weak.lock();
        //                     if (!locked) return;
        //
        //                     if (!local_state->ready.exchange(true, std::memory_order_acq_rel)) {
        //                         using CurrentT = std::tuple_element_t<I, std::tuple<T...>>;
        //                         try {
        //                             if constexpr (std::is_same_v<CurrentT, void>) {
        //                                 promise->get_result_move();
        //                                 std::get<I>(local_state->result) = std::expected<void, std::exception_ptr>{};
        //                             } else {
        //                                 std::get<I>(local_state->result) = std::expected<CurrentT, std::exception_ptr>{
        //                                     promise->get_result_move()
        //                                 };
        //                             }
        //                         } catch (...) {
        //                             std::get<I>(local_state->result) = std::unexpected{std::current_exception()};
        //                         }
        //                         if (local_state->parent_promise) {
        //                             local_ctx->resume_promise_strong(local_state->parent_promise);
        //                         }
        //                     }
        //                 });
        //                 local_ctx->resume_promise_strong(promise);
        //             };
        //
        //             std::apply([&](auto &... t) {
        //                 [&]<size_t... Is>(std::index_sequence<Is...>) {
        //                     (setup_task(t, std::integral_constant<size_t, Is>{}), ...);
        //                 }(std::index_sequence_for<T...>{});
        //             }, local_state->kept_tasks);
        //         }
        //
        //         void await_resume() const noexcept {}
        //     };
        //
        //     co_await awaiter{&state, weak_self, info.context};
        //
        //     std::tuple<std::optional<std::expected<T, std::exception_ptr>>...> ret_val = std::move(state.result);
        //     co_return ret_val;
        // }
        //
        // template<template<typename> typename TaskType = task, typename T>
        // TaskType<std::optional<std::expected<T, std::exception_ptr>>> any_of(std::vector<TaskType<T>> tasks) {
        //     struct State {
        //         std::atomic_bool ready{false};
        //         promise_base *parent_promise = nullptr;
        //         std::optional<std::expected<T, std::exception_ptr>> result;
        //         std::vector<TaskType<T>> kept_tasks;
        //     };
        //
        //     State state;
        //     state.kept_tasks = std::move(tasks);
        //
        //     auto info = co_await get_current_coroutine_info();
        //     auto weak_self = info.self->weak_borrow();
        //
        //     struct awaiter {
        //         State *state;
        //         ref_counted_resource_weak_handle weak_self;
        //         execution_context *ctx;
        //
        //         bool await_ready() const noexcept { return state->kept_tasks.empty(); }
        //
        //         void await_suspend(std::coroutine_handle<> h) noexcept {
        //             auto *parent_promise = &std::coroutine_handle<promise_base>::from_address(h.address()).promise();
        //             state->parent_promise = parent_promise;
        //             auto ref = parent_promise->borrow();
        //
        //             State *local_state = state;
        //             auto local_weak = weak_self;
        //             auto *local_ctx = ctx;
        //
        //             for (auto &task: local_state->kept_tasks) {
        //                 auto *promise = task.get_promise();
        //                 promise->set_execution_context(local_ctx);
        //
        //                 promise->set_continuation([local_weak, local_state, local_ctx, promise]() mutable {
        //                     auto locked = local_weak.lock();
        //                     if (!locked) return;
        //
        //                     if (!local_state->ready.exchange(true, std::memory_order_acq_rel)) {
        //                         try {
        //                             if constexpr (std::is_same_v<T, void>) {
        //                                 promise->get_result_move();
        //                                 local_state->result = std::expected<void, std::exception_ptr>{};
        //                             } else {
        //                                 local_state->result = std::expected<T, std::exception_ptr>{
        //                                     promise->get_result_move()
        //                                 };
        //                             }
        //                         } catch (...) {
        //                             local_state->result = std::unexpected{std::current_exception()};
        //                         }
        //                         if (local_state->parent_promise) {
        //                             local_ctx->resume_promise_strong(local_state->parent_promise);
        //                         }
        //                     }
        //                 });
        //                 local_ctx->resume_promise_strong(promise);
        //             }
        //         }
        //
        //         void await_resume() const noexcept {}
        //     };
        //
        //     co_await awaiter{&state, weak_self, info.context};
        //
        //     std::optional<std::expected<T, std::exception_ptr>> ret_val = std::move(state.result);
        //     co_return ret_val;
        // }

        template<typename T, typename S>
        class ex_promise;

        template<typename T, typename S>
        class COROUTINE_AWAIT_ELIDABLE ex_task : public task_base<T> {
        public:
            using promise_type = ex_promise<T, S>;

            ex_task() = default;

            ex_task(ref_counted_resource_handle coroutine_handle) noexcept : task_base<T>(
                std::move(coroutine_handle)) {}

            ex_promise<T, S> *get_ex_promise() const {
                return static_cast<ex_promise<T, S> *>(this->get_promise_base());
            }

            task<T> into_task() && {
                return task<T>(std::move(this->m_coroutine_handle));
            }
        };

        template<typename T, typename S>
        class ex_promise : public promise<T> {
        public:
            S m_state;

            ex_task<T, S> get_return_object() {
                return ex_task<T, S>(ref_counted_resource_handle(this, resource_acquisition_semantics::adopt{}));
            }

        protected:
            void release_resources() noexcept override {
                if constexpr (requires { m_state.release_resources(); }) {
                    m_state.release_resources();
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

        template<typename StateType>
        struct intrusive_all_of_awaiter {
            StateType *state;
            ref_counted_resource_weak_handle weak_self;
            execution_context *ctx;
            promise_base *all_of_promise;

            bool await_ready() const noexcept { return false; }

            void await_suspend(std::coroutine_handle<>) noexcept {
                auto *local_state = state;
                auto local_weak = weak_self;
                auto *local_ctx = ctx;
                auto *target_promise = all_of_promise;

                auto setup_task = [&]<size_t I>(auto &task_item, std::integral_constant<size_t, I>) {
                    auto *promise = task_item.get_promise();
                    promise->set_execution_context(local_ctx);

                    promise->set_continuation([local_weak, promise]() mutable {
                        auto locked = local_weak.lock();
                        if (!locked) return;

                        auto *parent_prom = static_cast<ex_promise<typename StateType::RetType, StateType> *>(locked.
                            get());
                        auto *local_state = &parent_prom->m_state;
                        auto *local_ctx = parent_prom->get_execution_context();

                        if (local_state->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                            local_ctx->resume_promise_strong(parent_prom);
                        }
                    });
                    local_ctx->resume_promise_strong(promise);
                };

                std::apply([&](auto &... t) {
                    [&]<size_t... Is>(std::index_sequence<Is...>) {
                        (setup_task(t, std::integral_constant<size_t, Is>{}), ...);
                    }(std::index_sequence_for<decltype(t)...>{});
                }, local_state->non_throw_tasks);
            }

            void await_resume() const noexcept {}
        };

        template<typename StateType>
        struct intrusive_vector_all_of_awaiter {
            StateType *state;
            ref_counted_resource_weak_handle weak_self;
            execution_context *ctx;
            promise_base *all_of_promise;

            bool await_ready() const noexcept { return state->non_throw_tasks.empty(); }

            void await_suspend(std::coroutine_handle<>) noexcept {
                auto *local_state = state;
                auto local_weak = weak_self;
                auto *local_ctx = ctx;
                auto *target_promise = all_of_promise;

                for (auto &t: local_state->non_throw_tasks) {
                    auto *promise = t.get_promise();
                    promise->set_execution_context(local_ctx);

                    promise->set_continuation([local_weak, promise]() mutable {
                        auto locked = local_weak.lock();
                        if (!locked) return;

                        auto *parent_prom = static_cast<ex_promise<typename StateType::RetType, StateType> *>(locked.
                            get());
                        auto *local_state = &parent_prom->m_state;
                        auto *local_ctx = parent_prom->get_execution_context();

                        if (local_state->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                            local_ctx->resume_promise_strong(parent_prom);
                        }
                    });
                    local_ctx->resume_promise_strong(promise);
                }
            }

            void await_resume() const noexcept {}
        };

        template<typename StateType>
        struct intrusive_any_of_awaiter {
            StateType *state;
            ref_counted_resource_weak_handle weak_self;
            execution_context *ctx;
            promise_base *any_of_promise;

            bool await_ready() const noexcept { return false; }

            void await_suspend(std::coroutine_handle<>) noexcept {
                auto *local_state = state;
                auto local_weak = weak_self;
                auto *local_ctx = ctx;
                auto *target_promise = any_of_promise;

                auto setup_task = [&]<size_t I>(auto &task_item, std::integral_constant<size_t, I>) {
                    auto *promise = task_item.get_promise();
                    promise->set_execution_context(local_ctx);

                    promise->set_continuation([local_weak, promise]() mutable {
                        auto locked = local_weak.lock();
                        if (!locked) return;

                        auto *parent_prom = static_cast<ex_promise<typename StateType::RetType, StateType> *>(locked.
                            get());
                        auto *local_state = &parent_prom->m_state;
                        auto *local_ctx = parent_prom->get_execution_context();

                        if (!local_state->ready.exchange(true, std::memory_order_acq_rel)) {
                            using CurrentT = typename std::remove_pointer_t<decltype(promise)>::value_type;
                            try {
                                if constexpr (std::is_same_v<CurrentT, void>) {
                                    promise->get_result_move();
                                    std::get<I>(local_state->result) = std::expected<void, std::exception_ptr>{};
                                } else {
                                    std::get<I>(local_state->result) = std::expected<CurrentT, std::exception_ptr>{
                                        promise->get_result_move()
                                    };
                                }
                            } catch (...) {
                                std::get<I>(local_state->result) = std::unexpected{std::current_exception()};
                            }
                            local_ctx->resume_promise_strong(parent_prom);
                        }
                    });
                    local_ctx->resume_promise_strong(promise);
                };

                std::apply([&](auto &... t) {
                    [&]<size_t... Is>(std::index_sequence<Is...>) {
                        (setup_task(t, std::integral_constant<size_t, Is>{}), ...);
                    }(std::index_sequence_for<decltype(t)...>{});
                }, local_state->kept_tasks);
            }

            void await_resume() const noexcept {}
        };

        template<typename StateType>
        struct intrusive_vector_any_of_awaiter {
            StateType *state;
            ref_counted_resource_weak_handle weak_self;
            execution_context *ctx;
            promise_base *any_of_promise;

            bool await_ready() const noexcept { return state->kept_tasks.empty(); }

            void await_suspend(std::coroutine_handle<>) noexcept {
                auto *local_state = state;
                auto local_weak = weak_self;
                auto *local_ctx = ctx;
                auto *target_promise = any_of_promise;

                for (auto &task_item: local_state->kept_tasks) {
                    auto *promise = task_item.get_promise();
                    promise->set_execution_context(local_ctx);

                    promise->set_continuation([local_weak, promise]() mutable {
                        auto locked = local_weak.lock();
                        if (!locked) return;

                        auto *parent_prom = static_cast<ex_promise<typename StateType::RetType, StateType> *>(locked.
                            get());
                        auto *local_state = &parent_prom->m_state;
                        auto *local_ctx = parent_prom->get_execution_context();

                        if (!local_state->ready.exchange(true, std::memory_order_acq_rel)) {
                            using CurrentT = typename std::remove_pointer_t<decltype(promise)>::value_type;
                            try {
                                if constexpr (std::is_same_v<CurrentT, void>) {
                                    promise->get_result_move();
                                    local_state->result = std::expected<void, std::exception_ptr>{};
                                } else {
                                    local_state->result = std::expected<CurrentT, std::exception_ptr>{
                                        promise->get_result_move()
                                    };
                                }
                            } catch (...) {
                                local_state->result = std::unexpected{std::current_exception()};
                            }
                            local_ctx->resume_promise_strong(parent_prom);
                        }
                    });
                    local_ctx->resume_promise_strong(promise);
                }
            }

            void await_resume() const noexcept {}
        };

        template<template<typename> typename TaskType = task, typename... T>
        TaskType<std::tuple<std::expected<T, std::exception_ptr>...>> all_of(
            COROUTINE_AWAIT_ELIDABLE_ARGUMENT
            task_base<T> &&... tasks) {
            using RetType = std::tuple<std::expected<T, std::exception_ptr>...>;

            struct SharedState {
                using RetType = std::tuple<std::expected<T, std::exception_ptr>...>;
                std::atomic_size_t remaining{sizeof...(T)};
                std::tuple<task_base<std::expected<T, std::exception_ptr>>...> non_throw_tasks;
            };

            auto impl = [](task_base<T>... tasks_impl) -> ex_task<RetType, SharedState> {
                auto *base_promise = co_await get_current_promise();
                auto *ex_prom = static_cast<ex_promise<RetType, SharedState> *>(base_promise);
                SharedState *local_state = &ex_prom->m_state;

                local_state->non_throw_tasks = std::make_tuple(into_non_throw(std::move(tasks_impl))...);
                auto weak_self = ref_counted_resource_weak_handle(base_promise, resource_acquisition_semantics::copy{});

                co_await intrusive_all_of_awaiter<SharedState>{
                    local_state, weak_self, base_promise->get_execution_context(), base_promise
                };

                RetType ret_val = std::apply([](auto &&... t) {
                    return std::make_tuple(t.get_promise()->get_result_move()...);
                }, local_state->non_throw_tasks);

                co_return ret_val;
            };

            return impl(std::move(tasks)...).release_handle();
        }

        template<template<typename> typename TaskType = task, typename T>
        TaskType<std::vector<std::expected<T, std::exception_ptr>>> all_of(std::vector<task_base<T>> tasks) {
            using RetType = std::vector<std::expected<T, std::exception_ptr>>;

            struct SharedState {
                using RetType = std::vector<std::expected<T, std::exception_ptr>>;
                std::atomic_size_t remaining{0};
                std::vector<task_base<std::expected<T, std::exception_ptr>>> non_throw_tasks;
            };

            auto impl = [](std::vector<task_base<T>> tasks_impl) -> ex_task<RetType, SharedState> {
                auto *base_promise = co_await get_current_promise();
                auto *ex_prom = static_cast<ex_promise<RetType, SharedState> *>(base_promise);
                SharedState *local_state = &ex_prom->m_state;

                local_state->remaining.store(tasks_impl.size(), std::memory_order_relaxed);
                local_state->non_throw_tasks.reserve(tasks_impl.size());
                for (auto &t: tasks_impl) {
                    local_state->non_throw_tasks.push_back(into_non_throw<TaskType, T>(std::move(t)));
                }

                auto weak_self = ref_counted_resource_weak_handle(base_promise, resource_acquisition_semantics::copy{});

                co_await intrusive_vector_all_of_awaiter<SharedState>{
                    local_state, weak_self, base_promise->get_execution_context(), base_promise
                };

                RetType results;
                results.reserve(local_state->non_throw_tasks.size());
                for (auto &t: local_state->non_throw_tasks) {
                    results.push_back(t.get_promise()->get_result_move());
                }

                co_return results;
            };

            return impl(std::move(tasks)).release_handle();
        }


        template<template<typename> typename TaskType = task, typename... T>
        TaskType<std::tuple<std::optional<std::expected<T, std::exception_ptr>>...>> any_of(
            COROUTINE_AWAIT_ELIDABLE_ARGUMENT
            task_base<T> &&... tasks) {
            using RetType = std::tuple<std::optional<std::expected<T, std::exception_ptr>>...>;

            struct SharedState {
                std::atomic_int sync{0};
                std::tuple<task_base<T>...> kept_tasks;
                RetType result;

                void release_resources() noexcept {
                    std::apply([](auto &... t) {
                        (t.reset(), ...);
                    }, kept_tasks);
                }
            };

            auto impl = [](task_base<T>... tasks_impl) -> ex_task<RetType, SharedState> {
                auto *base_promise = co_await get_current_promise();
                auto *ex_prom = static_cast<ex_promise<RetType, SharedState> *>(base_promise);
                SharedState *local_state = &ex_prom->m_state;

                local_state->kept_tasks = std::make_tuple(std::move(tasks_impl)...);
                execution_context *local_ctx = base_promise->get_execution_context();

                struct awaiter {
                    SharedState *state;
                    execution_context *ctx;
                    promise_base *target;

                    bool await_ready() const noexcept { return false; }

                    void await_suspend(std::coroutine_handle<>) noexcept {
                        auto setup_task = [this]<size_t I>(auto &task_item,
                                                           std::integral_constant<size_t, I>) -> promise_base * {
                            auto *promise = task_item.get_promise();
                            promise->set_execution_context(ctx);
                            auto weak_p = ref_counted_resource_weak_handle(
                                target, resource_acquisition_semantics::copy{});

                            promise->set_continuation(
                                [this_state = state, this_ctx = ctx, this_target = target, weak_p = std::move(weak_p),
                                    promise]() mutable {
                                    auto locked_parent = weak_p.lock();
                                    if (!locked_parent) {
                                        return;
                                    }

                                    int prev = this_state->sync.fetch_or(1, std::memory_order_acq_rel);

                                    if (!(prev & 1)) {
                                        using CurrentT = typename std::remove_pointer_t<decltype(promise)>::value_type;
                                        try {
                                            if constexpr (std::is_same_v<CurrentT, void>) {
                                                promise->get_result_move();
                                                std::get<I>(this_state->result) = std::expected<void,
                                                    std::exception_ptr>{};
                                            } else {
                                                std::get<I>(this_state->result) = std::expected<CurrentT,
                                                    std::exception_ptr>{
                                                    promise->get_result_move()
                                                };
                                            }
                                        } catch (...) {
                                            std::get<I>(this_state->result) = std::unexpected{std::current_exception()};
                                        }

                                        if (prev & 2) {
                                            this_ctx->resume_promise_weak(this_target);
                                        }
                                    }
                                });
                            return promise;
                        };

                        auto local_promises = std::apply([setup_task](auto &&... t) {
                            return [&]<size_t... Is>(std::index_sequence<Is...>) {
                                return std::array<promise_base *, sizeof...(t)>{
                                    setup_task(t, std::integral_constant<size_t, Is>{})...
                                };
                            }(std::index_sequence_for<decltype(t)...>{});
                        }, state->kept_tasks);

                        for (auto *p: local_promises) {
                            if (p) {
                                ctx->resume_promise_weak(p);
                            }
                        }

                        int prev = state->sync.fetch_or(2, std::memory_order_acq_rel);

                        if (prev & 1) {
                            ctx->resume_promise_weak(target);
                        }
                    }

                    void await_resume() const noexcept {}
                };

                co_await awaiter{local_state, local_ctx, base_promise};

                RetType final_result = std::move(local_state->result);
                local_state->release_resources();
                co_return std::move(final_result);
            };

            return impl(std::move(tasks)...).release_handle();
        }

        template<template<typename> typename TaskType = task, typename T>
        TaskType<std::optional<std::expected<T, std::exception_ptr>>> any_of(std::vector<task_base<T>> tasks) {
            using RetType = std::optional<std::expected<T, std::exception_ptr>>;

            struct SharedState {
                using RetType = std::optional<std::expected<T, std::exception_ptr>>;
                std::atomic_bool ready{false};
                std::vector<task_base<T>> kept_tasks;
                RetType result;
                execution_context *ctx = nullptr;
                promise_base *target_promise = nullptr;
            };

            auto impl = [](std::vector<task_base<T>> tasks_impl) -> task<RetType> {
                auto *base_promise = co_await get_current_promise();
                auto state = std::make_shared<SharedState>();

                state->kept_tasks = std::move(tasks_impl);
                state->ctx = base_promise->get_execution_context();
                state->target_promise = base_promise;

                struct awaiter {
                    std::shared_ptr<SharedState> state;

                    bool await_ready() const noexcept { return state->kept_tasks.empty(); }

                    void await_suspend(std::coroutine_handle<>) noexcept {
                        auto local_state = state;

                        for (auto &task_item: local_state->kept_tasks) {
                            auto *promise = task_item.get_promise();
                            assert(
                                promise &&
                                "Fatal: Awaited an empty or moved-from task in any_of vector. This is a caller-side bug.");

                            promise->set_execution_context(local_state->ctx);

                            promise->set_continuation([local_state, promise]() mutable {
                                if (!local_state->ready.exchange(true, std::memory_order_acq_rel)) {
                                    using CurrentT = typename std::remove_pointer_t<decltype(promise)>::value_type;
                                    try {
                                        if constexpr (std::is_same_v<CurrentT, void>) {
                                            promise->get_result_move();
                                            local_state->result = std::expected<void, std::exception_ptr>{};
                                        } else {
                                            local_state->result = std::expected<CurrentT, std::exception_ptr>{
                                                promise->get_result_move()
                                            };
                                        }
                                    } catch (...) {
                                        local_state->result = std::unexpected{std::current_exception()};
                                    }
                                    local_state->ctx->resume_promise_strong(local_state->target_promise);
                                }
                            });
                            local_state->ctx->resume_promise_strong(promise);
                        }
                    }

                    void await_resume() const noexcept {}
                };

                co_await awaiter{state};

                RetType ret_val = std::move(state->result);
                state->kept_tasks.clear();
                co_return ret_val;
            };

            return impl(std::move(tasks)).release_handle();
        }

        // template<template<typename> typename TaskType = task, typename... T>
        // TaskType<std::tuple<std::optional<std::expected<T, std::exception_ptr>>...>> any_of(
        //     COROUTINE_AWAIT_ELIDABLE_ARGUMENT
        //     task_base<T> &&... tasks) {
        //     using RetType = std::tuple<std::optional<std::expected<T, std::exception_ptr>>...>;
        //
        //     struct SharedState {
        //         using RetType = std::tuple<std::optional<std::expected<T, std::exception_ptr>>...>;
        //         std::atomic_bool ready{false};
        //         std::tuple<task_base<T>...> kept_tasks;
        //         RetType result;
        //
        //         void release_resources() noexcept {
        //             kept_tasks = {};
        //         }
        //     };
        //
        //     auto impl = [](task_base<T>... tasks_impl) -> ex_task<RetType, SharedState> {
        //         auto *base_promise = co_await get_current_promise();
        //         auto *ex_prom = static_cast<ex_promise<RetType, SharedState> *>(base_promise);
        //         SharedState *local_state = &ex_prom->m_state;
        //
        //         local_state->kept_tasks = std::make_tuple(std::move(tasks_impl)...);
        //         auto weak_self = ref_counted_resource_weak_handle(base_promise, resource_acquisition_semantics::copy{});
        //
        //         co_await intrusive_any_of_awaiter<SharedState>{
        //             local_state, weak_self, base_promise->get_execution_context(), base_promise
        //         };
        //
        //         co_return std::move(local_state->result);
        //     };
        //
        //     return impl(std::move(tasks)...).release_handle();
        // }
        //
        // template<template<typename> typename TaskType = task, typename T>
        // TaskType<std::optional<std::expected<T, std::exception_ptr>>> any_of(std::vector<task_base<T>> tasks) {
        //     using RetType = std::optional<std::expected<T, std::exception_ptr>>;
        //
        //     struct SharedState {
        //         using RetType = std::optional<std::expected<T, std::exception_ptr>>;
        //         std::atomic_bool ready{false};
        //         std::vector<task_base<T>> kept_tasks;
        //         RetType result;
        //
        //         void release_resources() noexcept {
        //             kept_tasks.clear();
        //         }
        //     };
        //
        //     auto impl = [](std::vector<task_base<T>> tasks_impl) -> ex_task<RetType, SharedState> {
        //         auto *base_promise = co_await get_current_promise();
        //         auto *ex_prom = static_cast<ex_promise<RetType, SharedState> *>(base_promise);
        //         SharedState *local_state = &ex_prom->m_state;
        //
        //         local_state->kept_tasks = std::move(tasks_impl);
        //         auto weak_self = ref_counted_resource_weak_handle(base_promise, resource_acquisition_semantics::copy{});
        //
        //         co_await intrusive_vector_any_of_awaiter<SharedState>{
        //             local_state, weak_self, base_promise->get_execution_context(), base_promise
        //         };
        //
        //         co_return std::move(local_state->result);
        //     };
        //
        //     return impl(std::move(tasks)).release_handle();
        // }

        template<typename T> requires (!std::same_as<T, void>)
        class generator_promise : public promise_base {
        public:
            std::optional<T> get_result_move() {
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

            template<typename Self> requires (std::is_rvalue_reference_v<Self &&> && !std::is_const_v<
                                                  std::remove_reference_t<Self>>)
            auto with_context(this Self &&self, execution_context *ctx) {
                self.get_promise()->set_execution_context(ctx);
                return std::move(self);
            }

            std::optional<T> await_resume() {
                assert(this->m_coroutine_handle);
                auto *promise = get_promise();
                return promise->get_resource_move();
            }

            auto by_promise(promise_base *parent_promise) {
                assert(parent_promise);

                struct awaiter {
                    generator m_generator;
                    promise_base *m_parent_promise;

                    bool await_ready() const noexcept {
                        return false;
                    }

                    void await_suspend(std::coroutine_handle<> h) noexcept {
                        execution_context *context = m_parent_promise->get_execution_context();
                        assert(context);

                        auto *promise = m_generator.get_promise();
                        if (!promise->get_execution_context()) {
                            promise->set_execution_context(context);
                        }

                        auto weak_parent = ref_counted_resource_weak_handle(
                            m_parent_promise, resource_acquisition_semantics::copy{});

                        auto *local_ctx = promise->get_execution_context();

                        promise->set_continuation([weak_parent = std::move(weak_parent)]() mutable {
                            auto parent = weak_parent.lock();
                            if (!parent) {
                                return;
                            }
                            auto *p = static_cast<promise_base *>(parent.get());
                            p->get_execution_context()->resume_promise_strong(p);
                        });

                        local_ctx->resume_promise_strong(promise);
                    }

                    decltype(auto) await_resume() {
                        return m_generator.get_promise()->get_result_move();
                    }
                };

                return awaiter{std::move(*this), parent_promise};
            }
        };

#if __has_include(<generator>)
        template
        <
            typename T>
        generator<T> asyncify(std::generator<T> gen) {
            for (auto &&value: gen) {
                co_yield value;
            }
        }
#endif

        template
        <
            template
            <
                typename > typename TaskType = task, typename Fn>

        auto asyncify_function(Fn fn) {
            return [fn = std::move(fn)]<typename... Args>(
                Args &&... args) mutable -> TaskType<std::invoke_result_t<Fn, Args...>> {
                co_return co_await fn(std::forward<Args>(args)...);
            };
        }

        template
        <
            typename T>
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

    task<void> sleep_for(std::chrono::milliseconds duration) {
        std::this_thread::sleep_for(duration);
        co_return;
    }

    namespace _details {
        namespace _pipe_utils {
            template<typename T>
            concept is_future = std::derived_from<T, task_base_flag>;

            struct coroutine_pipe_operand_flag {};

            template<typename T>
            concept is_coroutine_pipe_operand = std::derived_from<T, coroutine_pipe_operand_flag>;

            struct timeout_t : public coroutine_pipe_operand_flag {
            private:
                std::chrono::milliseconds m_duration;

            public:
                explicit timeout_t(std::chrono::milliseconds duration) : m_duration(duration) {}

                template<template<typename> typename TaskType, typename T>
                auto operator()(COROUTINE_AWAIT_ELIDABLE_ARGUMENT TaskType<T> &&future) const -> TaskType<
                    std::conditional_t<std::is_same_v<T, void>, bool, std::optional<T>>
                > requires is_future<TaskType<T>> {
                    auto [res, timeout] = co_await any_of<task>(std::move(future), sleep_for(m_duration));
                    if (res.has_value()) {
                        if constexpr (std::is_same_v<T, void>) {
                            if (res->has_value()) {
                                co_return true;
                            }
                            co_return false;
                        } else {
                            if (res->has_value()) {
                                co_return std::move(res->value());
                            }
                            std::rethrow_exception(res->error());
                        }
                    }
                    if constexpr (std::is_same_v<T, void>) {
                        co_return false;
                    } else {
                        co_return std::nullopt;
                    }
                }
            };

            inline timeout_t with_timeout(std::chrono::milliseconds duration) {
                return timeout_t(duration);
            }

            struct timeout_or_throw_t : public coroutine_pipe_operand_flag {
            private:
                std::chrono::milliseconds m_duration;

            public:
                explicit timeout_or_throw_t(std::chrono::milliseconds duration) : m_duration(duration) {}

                template<template<typename> typename TaskType, typename T>
                auto operator()(COROUTINE_AWAIT_ELIDABLE_ARGUMENT TaskType<T> &&future) const -> TaskType<T> requires
                    is_future<TaskType<T>> {
                    auto result = co_await any_of<task>(std::move(future), sleep_for(m_duration));
                    if (auto res = std::move(std::get<0>(result))) {
                        if (res.has_value()) {
                            if constexpr (std::is_same_v<T, void>) {
                                co_return;
                            } else {
                                co_return std::move(res.value());
                            }
                        }
                        std::rethrow_exception(res.error());
                    }
                    throw std::runtime_error("Operation timed out");
                }
            };

            inline timeout_or_throw_t with_timeout_throw(std::chrono::milliseconds duration) {
                return timeout_or_throw_t(duration);
            }
        }
    }

    using _details::_pipe_utils::with_timeout;
    using _details::_pipe_utils::with_timeout_throw;
}

template<coroutine::_details::_pipe_utils::is_future T, coroutine::_details::_pipe_utils::is_coroutine_pipe_operand
    Op>
auto operator|(COROUTINE_AWAIT_ELIDABLE_ARGUMENT T &&future,
               Op &&op) -> decltype(std::forward<Op>(op)(std::forward<T>(future))) {
    co_return co_await std::forward<Op>(op)(std::forward<T>(future));
}
