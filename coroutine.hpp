#pragma once

// A small coroutine library: reference-counted tasks scheduled on thread pools, with
// all_of / any_of combinators, cancellation by dropping the last strong reference, and
// (on Clang) heap-allocation elision of awaited child coroutines (HALO).
//
// Ownership model (read this first)
// ---------------------------------
// Every coroutine frame owns a promise derived from `promise_base`, which is reference counted
// like std::shared_ptr / std::weak_ptr (`pin_resource_base`):
//
//   * strong references keep the coroutine *running*. When the last strong reference goes away the
//     coroutine is cancelled: `release_resources()` drops the strong references it holds to its own
//     children (cancelling them in turn), and every later attempt to resume it fails to `lock()`.
//   * weak references keep the frame *memory* alive. The frame is destroyed when the last weak
//     reference goes away (the strong references collectively hold one weak reference).
//
// Who holds what:
//   * A task object (`task<T>`, ...) holds a strong reference to its coroutine.
//   * A parent awaiting a child holds the child strongly (its awaiter, or the shared state of
//     all_of / any_of). A child never holds its parent strongly, so dropping a parent cancels its
//     children.
//   * A child holds its parent weakly (`m_owner_handle`) and only releases it *after* its own frame
//     has been destroyed. With HALO a child's frame may live inside its parent's frame, so the
//     parent's frame must outlive the child's.
//   * Whoever resumes a coroutine (the thread-pool job) holds a strong reference for the duration of
//     the resumption, so a running coroutine is never destroyed underneath itself.
//   * A non-cancelable task pins itself (`m_self_handle`) once it is awaited, so it runs to
//     completion even if its parent is cancelled. A `cancelable_task` does not, and stops at its next
//     suspension point once its parent drops it.
//
// Hand-off rule ([expr.await]/5.1)
// --------------------------------
// A coroutine is already "suspended" when `await_suspend` starts running. As soon as
// `await_suspend` publishes the coroutine (starts work that will resume it), another thread may
// resume it, finish the co_await expression and destroy the awaiter, or even run the coroutine to
// completion, before `await_suspend` returns. So after the hand-off an `await_suspend` must not
// touch the awaiter, the promise or anything else that the resumed coroutine may release. Every
// awaiter below is written to follow this rule; look for "hand-off" comments.

#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <concepts>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <format>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <semaphore>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#if __has_include(<generator>)
#include <generator>
#endif

#include "thread_pool/thread_pool.h"

// HALO (heap allocation elision for awaited coroutines). Only Clang implements these attributes.
// The ownership model above (children hold their owner weakly until their own frame is gone) is what
// makes elision safe with cancellation.
#ifndef COROUTINE_ENABLE_HALO
#define COROUTINE_ENABLE_HALO 1
#endif
#if COROUTINE_ENABLE_HALO && defined(__clang__)
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

        // -----------------------------------------------------------------------------------------
        // Reference counting
        // -----------------------------------------------------------------------------------------

        // Strong/weak reference count embedded in every promise (see "Ownership model" above).
        class pin_resource_base {
        public:
            pin_resource_base() = default;

            void add_strong_reference() noexcept {
                [[maybe_unused]] auto old = m_strong_count.fetch_add(1, std::memory_order_relaxed);
                assert(old > 0 && "adding a strong reference to a released resource");
            }

            void add_weak_reference() noexcept {
                [[maybe_unused]] auto old = m_weak_count.fetch_add(1, std::memory_order_relaxed);
                assert(old > 0 && "adding a weak reference to a destroyed resource");
            }

            void release_strong_reference() noexcept {
                const auto old = m_strong_count.fetch_sub(1, std::memory_order_release);
                assert(old > 0 && "releasing a strong reference that is not held");
                if (old == 1) {
                    std::atomic_thread_fence(std::memory_order_acquire);
                    run_or_defer(pending_kind::release);
                }
            }

            void release_weak_reference() noexcept {
                const auto old = m_weak_count.fetch_sub(1, std::memory_order_release);
                assert(old > 0 && "releasing a weak reference that is not held");
                if (old == 1) {
                    std::atomic_thread_fence(std::memory_order_acquire);
                    run_or_defer(pending_kind::destroy);
                }
            }

            // Upgrades a weak reference: adds a strong reference unless the strong count already hit 0.
            bool try_add_strong_reference() noexcept {
                auto count = m_strong_count.load(std::memory_order_relaxed);
                while (count != 0) {
                    if (m_strong_count.compare_exchange_weak(count, count + 1, std::memory_order_acquire,
                                                             std::memory_order_relaxed)) {
                        return true;
                    }
                }
                return false;
            }

        protected:
            ~pin_resource_base() = default;

            // Called once when the last strong reference is released (cancellation or completion).
            virtual void release_resources() noexcept = 0;

            // Called once when the last weak reference is released; must destroy the object.
            virtual void destroy() noexcept = 0;

        protected:
            // Set (by promise_base) once this object holds a reference to its owner; see run_or_defer.
            void mark_has_owner() noexcept {
                m_has_owner = true;
            }

        private:
            // Releasing one coroutine releases its children, destroying a frame releases its owner, and
            // so on. Done recursively, cancelling a chain of 20000 coroutines overflows the stack. So the
            // outermost release on a thread drains a thread-local list in a loop, and releases triggered
            // while it does so are queued instead of run. The outermost call still returns only once
            // everything has been released, so callers (e.g. await_resume dropping a child) see the same
            // ordering as before.
            //
            // Only objects that hold their owner may be queued. With HALO an object's memory can be a slot
            // in another frame; the owner reference is what keeps that frame alive until the object is
            // gone. An object without one (a child that was created but never started, or a heap-allocated
            // root) must be handled right away: it may be released by the destruction of the very frame
            // that contains it, and that frame's memory is freed as soon as its destruction returns. Such
            // objects never form long chains (their own children hold them as owner), so the recursion
            // stays shallow.
            enum class pending_kind : uint8_t {
                release, // last strong reference gone: release_resources(), then the group's weak reference
                destroy, // last weak reference gone: destroy()
            };

            struct release_queue {
                pin_resource_base *head;
                bool draining;
            };

            inline static thread_local release_queue t_release_queue{nullptr, false};

            static void process(pin_resource_base *resource, pending_kind kind) noexcept {
                if (kind == pending_kind::release) {
                    resource->release_resources();
                    resource->release_weak_reference(); // the weak reference held by the strong references
                } else {
                    resource->destroy(); // `resource` is gone after this
                }
            }

            void run_or_defer(pending_kind kind) noexcept {
                auto &queue = t_release_queue;
                if (queue.draining && m_has_owner) {
                    // An object is queued at most once at a time: its weak count cannot reach zero while
                    // its `release` entry is pending, because that entry still owns the group's weak
                    // reference.
                    m_pending_kind = kind;
                    m_next_pending = std::exchange(queue.head, this);
                    return;
                }

                const bool outermost = !queue.draining;
                queue.draining = true;
                process(this, kind);
                if (outermost) {
                    while (pin_resource_base *resource = queue.head) {
                        queue.head = resource->m_next_pending;
                        process(resource, resource->m_pending_kind);
                    }
                    queue.draining = false;
                }
            }

            pin_resource_base *m_next_pending = nullptr;
            pending_kind m_pending_kind = pending_kind::release;
            bool m_has_owner = false;

            std::atomic_uint32_t m_strong_count{1};
            std::atomic_uint32_t m_weak_count{1};
        };

        inline namespace resource_acquisition_semantics {
            // Take over a reference the caller already owns.
            struct adopt {
            };

            // Add a new reference.
            struct copy {
            };
        }

        class ref_counted_resource_weak_handle;

        // Strong reference (like std::shared_ptr).
        class ref_counted_resource_handle {
            friend ref_counted_resource_weak_handle;

        public:
            ref_counted_resource_handle() noexcept = default;

            ref_counted_resource_handle(pin_resource_base *resource, adopt) noexcept : m_resource(resource) {
            }

            ref_counted_resource_handle(pin_resource_base *resource, copy) noexcept : m_resource(resource) {
                if (m_resource) {
                    m_resource->add_strong_reference();
                }
            }

            ref_counted_resource_handle(const ref_counted_resource_handle &other) noexcept
                : ref_counted_resource_handle(other.m_resource, copy{}) {
            }

            ref_counted_resource_handle(ref_counted_resource_handle &&other) noexcept
                : m_resource(std::exchange(other.m_resource, nullptr)) {
            }

            ref_counted_resource_handle &operator=(ref_counted_resource_handle other) noexcept {
                std::swap(m_resource, other.m_resource);
                return *this;
            }

            ~ref_counted_resource_handle() noexcept {
                reset();
            }

            explicit operator bool() const noexcept {
                return m_resource != nullptr;
            }

            [[nodiscard]] bool has_value() const noexcept {
                return m_resource != nullptr;
            }

            void reset() noexcept {
                // Clear the member before releasing: releasing may destroy the object that owns *this.
                if (auto *resource = std::exchange(m_resource, nullptr)) {
                    resource->release_strong_reference();
                }
            }

            [[nodiscard]] pin_resource_base *get() const noexcept {
                return m_resource;
            }

        private:
            pin_resource_base *m_resource = nullptr;
        };

        // Weak reference (like std::weak_ptr).
        class ref_counted_resource_weak_handle {
        public:
            ref_counted_resource_weak_handle() noexcept = default;

            ref_counted_resource_weak_handle(pin_resource_base *resource, copy) noexcept : m_resource(resource) {
                if (m_resource) {
                    m_resource->add_weak_reference();
                }
            }

            ref_counted_resource_weak_handle(const ref_counted_resource_handle &strong) noexcept
                : ref_counted_resource_weak_handle(strong.m_resource, copy{}) {
            }

            ref_counted_resource_weak_handle(const ref_counted_resource_weak_handle &other) noexcept
                : ref_counted_resource_weak_handle(other.m_resource, copy{}) {
            }

            ref_counted_resource_weak_handle(ref_counted_resource_weak_handle &&other) noexcept
                : m_resource(std::exchange(other.m_resource, nullptr)) {
            }

            ref_counted_resource_weak_handle &operator=(ref_counted_resource_weak_handle other) noexcept {
                std::swap(m_resource, other.m_resource);
                return *this;
            }

            ~ref_counted_resource_weak_handle() noexcept {
                reset();
            }

            // Returns a strong reference, or an empty handle if the resource was already released.
            [[nodiscard]] ref_counted_resource_handle lock() const noexcept {
                if (m_resource && m_resource->try_add_strong_reference()) {
                    return {m_resource, adopt{}};
                }
                return {};
            }

            void reset() noexcept {
                if (auto *resource = std::exchange(m_resource, nullptr)) {
                    resource->release_weak_reference();
                }
            }

            [[nodiscard]] bool is_null() const noexcept {
                return m_resource == nullptr;
            }

            // The object is not necessarily alive in the "strong" sense; only its memory is.
            [[nodiscard]] pin_resource_base *get_raw() const noexcept {
                return m_resource;
            }

        private:
            pin_resource_base *m_resource = nullptr;
        };

        // -----------------------------------------------------------------------------------------
        // Diagnostics (used by the stress tests to detect leaks)
        // -----------------------------------------------------------------------------------------

        inline std::atomic_size_t g_promise_count{0};

        inline size_t debug_get_active_promise_count() noexcept {
            return g_promise_count.load(std::memory_order_acquire);
        }

        // -----------------------------------------------------------------------------------------
        // promise_base
        // -----------------------------------------------------------------------------------------

        // Awaitables that need access to the awaiting promise derive from this; promise_base's
        // await_transform hands them the promise through `by_promise(promise_base *)`.
        struct abstract_awaitable_base {
        };

        template<typename T>
        concept is_awaitable = std::derived_from<std::remove_cvref_t<T>, abstract_awaitable_base>;

        // Extension point: specialize to make other types awaitable inside our coroutines.
        // `do_transform` converts a value into something co_await-able; set `need_forwarding` if
        // the result should itself go through await_transform again.
        template<typename T>
        struct promise_base_transformer {
            constexpr static bool need_forwarding = false;

            template<typename Awaitable>
            static decltype(auto) do_transform(Awaitable &&awaitable) {
                return std::forward<Awaitable>(awaitable);
            }
        };

        class promise_base : public pin_resource_base {
            template<typename T>
            friend class task_base;

        public:
            // Called when the coroutine finishes, or by a generator on each co_yield.
            using continuation_type = std::move_only_function<void()>;

            enum task_hint : uint32_t {
                e_none = 0,
                e_io = 1 << 0, // resume on the I/O pool (blocking work)
            };

            promise_base() {
                g_promise_count.fetch_add(1, std::memory_order_relaxed);
            }

            promise_base(const promise_base &) = delete;

            promise_base &operator=(const promise_base &) = delete;

            // ---- awaiting ----

            template<is_awaitable Awaitable>
            decltype(auto) await_transform(Awaitable &&awaitable) {
                return std::forward<Awaitable>(awaitable).by_promise(this);
            }

            template<typename T> requires (!is_awaitable<T>)
            decltype(auto) await_transform(T &&value) {
                using transformer_type = promise_base_transformer<std::remove_cvref_t<T>>;
                if constexpr (transformer_type::need_forwarding) {
                    return await_transform(transformer_type::do_transform(std::forward<T>(value)));
                } else {
                    return transformer_type::do_transform(std::forward<T>(value));
                }
            }

            // ---- continuation ----

            void set_continuation(continuation_type continuation) {
                m_continuation = std::move(continuation);
            }

            continuation_type take_continuation() noexcept {
                return std::exchange(m_continuation, nullptr);
            }

            // ---- references ----

            [[nodiscard]] ref_counted_resource_handle borrow() const noexcept {
                return {const_cast<promise_base *>(this), copy{}};
            }

            [[nodiscard]] ref_counted_resource_weak_handle weak_borrow() const noexcept {
                return {const_cast<promise_base *>(this), copy{}};
            }

            // Sets the coroutine that may contain our frame (see "Ownership model").
            void set_owner(ref_counted_resource_weak_handle owner_handle) noexcept {
                m_owner_handle = std::move(owner_handle);
                if (!m_owner_handle.is_null()) {
                    mark_has_owner();
                }
            }

            // ---- properties ----

            void set_execution_context(execution_context *context) noexcept {
                m_execution_context = context;
            }

            [[nodiscard]] execution_context *get_execution_context() const noexcept {
                return m_execution_context;
            }

            [[nodiscard]] task_hint get_task_hint() const noexcept {
                return m_task_hint;
            }

            void set_task_hint(task_hint hint) noexcept {
                m_task_hint = hint;
            }

            [[nodiscard]] bool is_cancelable() const noexcept {
                return m_cancelable;
            }

            void set_cancelable(bool value) noexcept {
                m_cancelable = value;
            }

            // Called once our frame has been destroyed and our owner released (see destroy). any_of uses
            // it to wait until every child, with everything below it, is gone.
            void set_on_destroyed(continuation_type on_destroyed) noexcept {
                m_on_destroyed = std::move(on_destroyed);
            }

            [[nodiscard]] std::coroutine_handle<> get_coroutine_handle() const noexcept {
                return m_coroutine_handle;
            }

            // ---- coroutine interface ----

            // Converts into whichever task type the coroutine returns; the task adopts the initial
            // strong reference.
            struct return_object_builder {
                promise_base *m_promise;

                template<typename Task>
                operator Task() {
                    assert(m_promise);
                    return Task(ref_counted_resource_handle(std::exchange(m_promise, nullptr), adopt{}));
                }
            };

            template<typename Self>
            return_object_builder get_return_object(this Self &self) {
                self.m_coroutine_handle = std::coroutine_handle<Self>::from_promise(self);
                return {static_cast<promise_base *>(&self)};
            }

            // Tasks are lazy: nothing runs until the task is awaited, spawned or blocked on.
            constexpr static std::suspend_always initial_suspend() noexcept {
                return {};
            }

            // What a finished coroutine still has to do once it is no longer running.
            struct finished_work {
                continuation_type continuation;
                ref_counted_resource_handle pin;
            };

            // Set by final_awaiter, consumed by the resumer (see `resume_and_finish`) on the same thread.
            inline static thread_local finished_work t_finished_work{};

            // Finishing is handed over to the resumer (resume_and_finish) instead of running here: the
            // continuation may make our parent move on, and the resumer runs it once the coroutine has
            // fully suspended and `resume()` has returned, so nothing of ours is still executing then.
            // (Parents wait for our destruction, not for this, before they reuse our HALO slot.)
            struct final_awaiter : std::suspend_always {
                promise_base *m_promise;

                void await_suspend(std::coroutine_handle<>) const noexcept {
                    assert(!t_finished_work.continuation && !t_finished_work.pin);
                    t_finished_work.continuation = m_promise->take_continuation();
                    t_finished_work.pin = std::move(m_promise->m_self_handle);
                }
            };

            // Resumes a coroutine on the current thread, then runs whatever it left in t_finished_work.
            // `strong` is the resumer's reference; it keeps the frame alive while the coroutine runs.
            static void resume_and_finish(ref_counted_resource_handle strong) {
                if (!strong) {
                    return;
                }
                const std::coroutine_handle<> handle = static_cast<promise_base *>(strong.get())->get_coroutine_handle();
                assert(handle && !handle.done() && "resuming a coroutine that is not suspended");
                handle.resume();
                // Do not inspect the coroutine here (e.g. handle.done()): if it suspended at a co_await, it
                // may already have been handed to, and resumed by, another thread.

                // The continuation runs while we still hold the coroutine, so it can read the result. That
                // our references outlive it is harmless: whoever waits for this coroutine waits for its
                // destruction (see task_base::by_promise and wait_children_destroyed).
                auto work = std::exchange(t_finished_work, {});
                if (work.continuation) {
                    work.continuation();
                }
                work.pin.reset();
                strong.reset();
            }

            final_awaiter final_suspend() noexcept {
                return {{}, this};
            }

            // ---- allocation diagnostics ----

            inline static std::atomic_size_t g_heap_alloc_count{0};

            void *operator new(std::size_t size) {
                g_heap_alloc_count.fetch_add(1, std::memory_order_relaxed);
                return ::operator new(size);
            }

            void operator delete(void *ptr) noexcept {
                ::operator delete(ptr);
            }

        protected:
            ~promise_base() {
                g_promise_count.fetch_sub(1, std::memory_order_release);
            }

        public:
            // Something this coroutine is suspended on that must be let go of if it is cancelled: a child
            // it awaits (awaited_child) or a timer (delay). The node lives in the awaiter, inside our
            // frame, and is linked from await_suspend until await_resume. Usually there is one; there can
            // be several when one expression has several co_awaits (`co_await a() + co_await b()`): the
            // operands are unsequenced, and MSVC suspends for b before running a's await_resume.
            struct cancel_hook {
                cancel_hook *next = nullptr;

                virtual void on_cancel() noexcept = 0;

            protected:
                ~cancel_hook() = default;
            };

            // A child being awaited: cancelling us releases it. It is also released once it has finished
            // (by its continuation, on the child's thread); the flag makes sure only one of the two does.
            struct awaited_child final : cancel_hook {
                ref_counted_resource_handle child;
                std::atomic_bool released{false};

                explicit awaited_child(ref_counted_resource_handle handle) noexcept : child(std::move(handle)) {
                }

                void release_child() noexcept {
                    if (!released.exchange(true, std::memory_order_acq_rel)) {
                        child.reset();
                    }
                }

                void on_cancel() noexcept override {
                    release_child();
                }
            };

            // Both only run while this coroutine is running (await_suspend before the hand-off,
            // await_resume after resumption), so they never race with each other or with
            // release_resources, which only runs once nobody can resume us any more.
            void link_cancel_hook(cancel_hook *node) noexcept {
                node->next = std::exchange(m_cancel_hooks, node);
            }

            void unlink_cancel_hook(cancel_hook *node) noexcept {
                for (auto **link = &m_cancel_hooks; *link; link = &(*link)->next) {
                    if (*link == node) {
                        *link = node->next;
                        return;
                    }
                }
                assert(false && "cancel hook is not linked");
            }

        protected:
            // Last strong reference gone: let go of whatever we are suspended on. Our frame cannot be
            // destroyed during the walk: the strong references' weak reference is only released after we
            // return.
            void release_resources() noexcept override {
                for (auto *node = m_cancel_hooks; node; node = node->next) {
                    node->on_cancel();
                }
            }

            // Last weak reference gone: destroy the frame, then let go of the owner, then tell whoever
            // waits for us. The owner must be released *after* the frame, because with HALO our frame may
            // live inside the owner's; and before the notification, because the notified coroutine may
            // finish and have its own slot reused right away.
            void destroy() noexcept override {
                auto owner = std::move(m_owner_handle);
                auto on_destroyed = std::move(m_on_destroyed);
                m_continuation = nullptr;
                m_coroutine_handle.destroy();
                owner.reset();
                if (on_destroyed) {
                    on_destroyed();
                }
            }

            continuation_type m_continuation;
            continuation_type m_on_destroyed;
            ref_counted_resource_weak_handle m_owner_handle{};
            ref_counted_resource_handle m_self_handle{}; // pins a non-cancelable task while awaited
            cancel_hook *m_cancel_hooks = nullptr; // what we are currently suspended on
            execution_context *m_execution_context = nullptr;
            std::coroutine_handle<> m_coroutine_handle = nullptr;
            task_hint m_task_hint{e_none};
            bool m_cancelable = false;
        };
    }

    // ---------------------------------------------------------------------------------------------
    // execution_context
    // ---------------------------------------------------------------------------------------------

    class execution_context {
    public:
        virtual ~execution_context() = default;

        // Schedules `promise` to be resumed; the resumption is skipped if it has been cancelled by then.
        void resume_promise(_details::promise_base *promise);

        virtual void resume_promise_weak(_details::ref_counted_resource_weak_handle weak_handle) = 0;

        virtual void resume_promise_strong(_details::ref_counted_resource_handle strong_handle) = 0;

        // Runs `task` on this context and blocks the calling thread until it finishes.
        template<template<typename...> typename TaskType, typename T, typename... Args>
        T block_on(TaskType<T, Args...> task);

        // Like block_on, but returns immediately with a std::future.
        template<template<typename...> typename TaskType, typename T, typename... Args>
        std::future<T> async_execute(TaskType<T, Args...> task);

        // Shared lock: the context cannot be destroyed while it is held.
        virtual void lock() = 0;

        virtual void unlock() = 0;

        // Blocks until no work is queued or running.
        virtual void wait_idle() = 0;

        // Starts `coroutine` detached; the context's destructor waits for it to finish.
        virtual void co_spawn(_details::task_base<void> coroutine) = 0;

        // ---- timers: the event-loop part of a context ----

        struct timer_id {
            std::chrono::steady_clock::time_point deadline;
            std::uint64_t sequence;

            auto operator<=>(const timer_id &) const = default;
        };

        // Resumes `coroutine` (unless it has been cancelled by then) once `deadline` has passed. The id is
        // written to `id` before the timer can fire: the caller is suspending and must not touch its
        // awaiter after this call returns (see the hand-off rule).
        virtual void add_timer(std::chrono::steady_clock::time_point deadline,
                               _details::ref_counted_resource_weak_handle coroutine, timer_id &id) = 0;

        // Removes a timer that has not fired yet; does nothing if it already has.
        virtual void cancel_timer(const timer_id &id) noexcept = 0;

        // RAII holder of the shared lock above.
        struct context_lock_holder_type {
            context_lock_holder_type(execution_context *ctx) : m_context(ctx) {
                m_context->lock();
            }

            context_lock_holder_type(const context_lock_holder_type &) = delete;

            context_lock_holder_type &operator=(const context_lock_holder_type &) = delete;

            context_lock_holder_type(context_lock_holder_type &&other) noexcept
                : m_context(std::exchange(other.m_context, nullptr)) {
            }

            context_lock_holder_type &operator=(context_lock_holder_type &&other) noexcept {
                if (this != &other) {
                    if (m_context) {
                        m_context->unlock();
                    }
                    m_context = std::exchange(other.m_context, nullptr);
                }
                return *this;
            }

            ~context_lock_holder_type() {
                if (m_context) {
                    m_context->unlock();
                }
            }

            [[nodiscard]] context_lock_holder_type clone() const {
                return {m_context};
            }

            [[nodiscard]] execution_context *get_context() const {
                return m_context;
            }

            execution_context *operator->() const {
                return m_context;
            }

        private:
            execution_context *m_context;
        };

        context_lock_holder_type lock_this() {
            return {this};
        }
    };

    inline void execution_context::resume_promise(_details::promise_base *promise) {
        assert(promise);
        resume_promise_weak(promise->weak_borrow());
    }

    namespace _details {
        // Schedules the coroutine behind `weak` and keeps no reference to it at all: the reference is
        // handed over to the scheduler. Every continuation that wakes a parent goes through here. Holding
        // on to the parent (e.g. `if (auto p = weak.lock()) resume(p);`) would keep it alive while it
        // runs on another thread; if it finished and its own parent continued, that parent could reuse
        // its HALO slot while we still referred to it. A cancelled coroutine is not resumed: the
        // scheduler's lock() fails.
        inline void schedule_resume(ref_counted_resource_weak_handle weak) {
            auto *promise = static_cast<promise_base *>(weak.get_raw()); // the weak reference keeps the memory
            promise->get_execution_context()->resume_promise_weak(std::move(weak));
        }
    }

    namespace _details {
        // -----------------------------------------------------------------------------------------
        // promise<T>: stores the result
        // -----------------------------------------------------------------------------------------

        inline namespace result_state {
            struct not_finished {
            };

            template<typename T>
            using finished = std::conditional_t<std::is_void_v<T>, std::monostate, T>;

            using uncaught_exception = std::exception_ptr;
        }

        template<typename T>
        class promise : public promise_base {
        public:
            using value_type = T;
            using result_variant = std::variant<not_finished, finished<T>, uncaught_exception>;

            // Moves the outcome out (used by the awaiter of a direct co_await, see task_base::by_promise).
            result_variant take_result() noexcept {
                return std::move(m_result);
            }

            // The value of an outcome taken with take_result, or its exception rethrown.
            static T unwrap(result_variant &&result) {
                return std::visit(overloaded{
                                      [](not_finished &) -> T {
                                          throw std::runtime_error("Result not set.");
                                      },
                                      [](finished<T> &value) -> T {
                                          return std::move(value);
                                      },
                                      [](uncaught_exception &e) -> T {
                                          std::rethrow_exception(e);
                                      }
                                  }, result);
            }

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

            template<std::convertible_to<T> U>
            void return_value(U value) {
                m_result.template emplace<finished<T>>(std::move(value));
            }

            void unhandled_exception() {
                m_result.template emplace<uncaught_exception>(std::current_exception());
            }

        private:
            std::variant<not_finished, finished<T>, uncaught_exception> m_result{not_finished{}};
        };

        template<>
        class promise<void> : public promise_base {
        public:
            using value_type = void;
            using result_variant = std::variant<not_finished, finished<void>, uncaught_exception>;

            result_variant take_result() noexcept {
                return std::move(m_result);
            }

            static void unwrap(result_variant &&result) {
                std::visit(overloaded{
                               [](not_finished &) {
                                   throw std::runtime_error("Result not set.");
                               },
                               [](finished<void> &) {
                               },
                               [](uncaught_exception &e) {
                                   std::rethrow_exception(e);
                               }
                           }, result);
            }

            void get_result_copy() const {
                std::visit(overloaded{
                               [](const not_finished &) {
                                   throw std::runtime_error("Result not set.");
                               },
                               [](const finished<void> &) {
                               },
                               [](const uncaught_exception &e) {
                                   std::rethrow_exception(e);
                               }
                           }, m_result);
            }

            void get_result_move() {
                get_result_copy();
            }

            void return_void() {
                m_result.template emplace<finished<void>>();
            }

            void unhandled_exception() {
                m_result.template emplace<uncaught_exception>(std::current_exception());
            }

        private:
            std::variant<not_finished, finished<void>, uncaught_exception> m_result{not_finished{}};
        };

        // -----------------------------------------------------------------------------------------
        // Task types
        // -----------------------------------------------------------------------------------------

        // Owns a strong reference to a coroutine.
        class future_base {
        public:
            future_base() noexcept = default;

            future_base(ref_counted_resource_handle coroutine_handle) noexcept
                : m_coroutine_handle(std::move(coroutine_handle)) {
            }

            future_base(const future_base &) = delete;

            future_base &operator=(const future_base &) = delete;

            future_base(future_base &&) noexcept = default;

            future_base &operator=(future_base &&) noexcept = default;

            explicit operator bool() const noexcept {
                return m_coroutine_handle.has_value();
            }

            [[nodiscard]] bool has_value() const noexcept {
                return m_coroutine_handle.has_value();
            }

            void reset() noexcept {
                m_coroutine_handle.reset();
            }

            ref_counted_resource_handle release_handle() noexcept {
                return std::move(m_coroutine_handle);
            }

        protected:
            [[nodiscard]] promise_base *get_promise_base() const noexcept {
                return static_cast<promise_base *>(m_coroutine_handle.get());
            }

            ref_counted_resource_handle m_coroutine_handle;
        };

        // The common part of every task type. Awaiting a task (inside one of our coroutines) goes
        // through `by_promise`, which transfers ownership of the task to the awaiting parent.
        template<typename T>
        class COROUTINE_AWAIT_ELIDABLE task_base : public abstract_awaitable_base, public future_base,
                                                   public std::suspend_always {
        public:
            using promise_type = promise<T>;
            using value_type = T;

            task_base() = default;

            task_base(ref_counted_resource_handle coroutine_handle) noexcept
                : future_base(std::move(coroutine_handle)) {
            }

            task_base(task_base &&) = default;

            task_base &operator=(task_base &&) = default;

            [[nodiscard]] promise<T> *get_promise() const {
                return static_cast<promise<T> *>(this->get_promise_base());
            }

            promise<T> *operator->() const {
                assert(m_coroutine_handle);
                return get_promise();
            }

            T get_result() const & {
                return checked_promise()->get_result_copy();
            }

            T get_result() && {
                return checked_promise()->get_result_move();
            }

            // `co_await task` inside one of our coroutines.
            //
            // The parent continues only once the child has been *destroyed*, not merely finished: with HALO
            // the child's frame is a slot in the parent's frame, reused as soon as the parent moves on, and a
            // finished child can still be referenced for a while (e.g. by a thread that resumed it earlier
            // and has not returned from resume() yet). So: when the child finishes, its continuation moves
            // the result into this awaiter and releases the parent's reference to the child; the child's
            // on_destroyed hook then resumes the parent.
            //
            // The awaiter owns the child until await_suspend; only then is the child pinned and made visible
            // to the parent's release_resources (cancel_hook). If the co_await expression is abandoned before
            // it suspends (an exception in another operand), the awaiter just drops its unstarted child.
            auto by_promise(promise_base *parent_promise) && {
                assert(parent_promise);
                checked_promise();

                struct awaiter {
                    promise_base *m_parent_promise;
                    promise_base::awaited_child m_node;
                    typename promise_type::result_variant m_result{};

                    [[nodiscard]] constexpr static bool await_ready() noexcept {
                        return false;
                    }

                    void await_suspend(std::coroutine_handle<>) noexcept {
                        execution_context *context = m_parent_promise->get_execution_context();
                        assert(context);

                        auto *child = static_cast<promise_type *>(m_node.child.get());
                        if (!child->is_cancelable()) {
                            child->m_self_handle = child->borrow(); // run to completion even if the parent is cancelled
                        }
                        m_parent_promise->link_cancel_hook(&m_node);

                        if (!child->get_execution_context()) {
                            child->set_execution_context(context);
                        }

                        child->set_owner(m_parent_promise->weak_borrow());
                        // Runs on the child's thread when it finishes, while the child is still referenced
                        // by its finisher. Our frame stays allocated: the child holds it as its owner.
                        child->set_continuation([this, child] {
                            m_result = child->take_result();
                            m_node.release_child();
                        });
                        child->set_on_destroyed([weak_parent = m_parent_promise->weak_borrow()]() mutable {
                            schedule_resume(std::move(weak_parent));
                        });

                        child->get_execution_context()->resume_promise(child);
                        // Hand-off: the child may already have finished, been destroyed and resumed us.
                    }

                    auto await_resume() {
                        m_parent_promise->unlink_cancel_hook(&m_node);
                        return promise_type::unwrap(std::move(m_result));
                    }
                };

                return awaiter{parent_promise, promise_base::awaited_child{std::move(this->m_coroutine_handle)}};
            }

            // Used only when a task is awaited by a coroutine whose promise is not a promise_base.
            T await_resume() {
                return checked_promise()->get_result_move();
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

            // Re-wraps the same coroutine in another task type, e.g. `.into<interruptable_task>()`.
            template<template<typename> typename TaskType>
            TaskType<T> into(COROUTINE_AWAIT_ELIDABLE_ARGUMENT this task_base<T> &&self) {
                return TaskType<T>(std::move(self).release_handle());
            }

        private:
            promise<T> *checked_promise() const {
                if (!this->m_coroutine_handle) {
                    throw std::runtime_error("No coroutine associated with this task.");
                }
                return get_promise();
            }
        };

        // Cancelled as soon as its last owner lets go of it (it does not pin itself when awaited).
        template<typename T>
        class COROUTINE_AWAIT_ELIDABLE cancelable_task : public task_base<T> {
        public:
            using typename task_base<T>::promise_type;

            cancelable_task() = default;

            cancelable_task(ref_counted_resource_handle &&coroutine_handle) noexcept
                : task_base<T>(std::move(coroutine_handle)) {
                auto *promise = this->get_promise();
                assert(promise);
                promise->set_cancelable(true);
            }

            cancelable_task(cancelable_task &&) = default;

            cancelable_task &operator=(cancelable_task &&) = default;

            void cancel() {
                this->m_coroutine_handle.reset();
            }
        };

        // Once awaited, runs to completion even if the awaiting coroutine is cancelled.
        template<typename T>
        class COROUTINE_AWAIT_ELIDABLE task : public task_base<T> {
        public:
            using typename task_base<T>::promise_type;

            task() = default;

            task(ref_counted_resource_handle &&coroutine_handle) noexcept
                : task_base<T>(std::move(coroutine_handle)) {
            }

            task(task &&) = default;

            task &operator=(task &&) = default;
        };

        // -----------------------------------------------------------------------------------------
        // Queries: `co_await get_current_...()` returns information about the running coroutine
        // without suspending.
        // -----------------------------------------------------------------------------------------

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

        inline get_promise_type get_current_promise() {
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

        inline get_coroutine_info_type get_current_coroutine_info() {
            return {};
        }

        // -----------------------------------------------------------------------------------------
        // Waiting without blocking a thread
        // -----------------------------------------------------------------------------------------

        // `co_await delay(d)` suspends the coroutine for `d` using the context's timer; no thread is
        // occupied meanwhile. If the coroutine is cancelled while it waits, the timer is removed, so the
        // coroutine can be destroyed right away instead of when the timer would have fired.
        struct delay_type : abstract_awaitable_base {
            std::chrono::steady_clock::duration m_duration;

            struct awaiter final : promise_base::cancel_hook {
                promise_base *m_promise;
                std::chrono::steady_clock::duration m_duration;
                execution_context::timer_id m_timer{};

                awaiter(promise_base *promise, std::chrono::steady_clock::duration duration) noexcept
                    : m_promise(promise), m_duration(duration) {
                }

                [[nodiscard]] bool await_ready() const noexcept {
                    return m_duration <= std::chrono::steady_clock::duration::zero();
                }

                void await_suspend(std::coroutine_handle<>) noexcept {
                    execution_context *context = m_promise->get_execution_context();
                    assert(context);
                    m_promise->link_cancel_hook(this);
                    context->add_timer(std::chrono::steady_clock::now() + m_duration, m_promise->weak_borrow(),
                                       m_timer);
                    // Hand-off: the timer may already have fired and resumed us on another thread.
                }

                void await_resume() noexcept {
                    if (!await_ready()) { // only linked if we actually suspended
                        m_promise->unlink_cancel_hook(this);
                    }
                }

                void on_cancel() noexcept override {
                    m_promise->get_execution_context()->cancel_timer(m_timer);
                }
            };

            [[nodiscard]] awaiter by_promise(promise_base *promise) const {
                return {promise, m_duration};
            }
        };

        inline delay_type delay(std::chrono::steady_clock::duration duration) {
            return {{}, duration};
        }

        // Waits until `ready()` returns true, checking it every `interval`. Between checks the coroutine is
        // suspended (no thread is blocked), so it can be cancelled at once. Use it to turn something that
        // would block (a std::future, a semaphore, ...) into something that can be interrupted.
        template<typename Predicate>
        cancelable_task<void> poll_until(Predicate ready,
                                         std::chrono::steady_clock::duration interval = std::chrono::milliseconds(1)) {
            while (!ready()) {
                co_await delay(interval);
            }
        }

        // -----------------------------------------------------------------------------------------
        // detach: run a task in the background
        // -----------------------------------------------------------------------------------------

        // Where a detached task finds the coroutine waiting for it. Shared (not a reference to the waiter
        // held by the task), so that cancelling the waiter can cut the link: the waiter can then be
        // destroyed at once instead of when the detached task finishes.
        struct detach_link {
            std::mutex mutex;
            ref_counted_resource_weak_handle waiter;
        };

        struct detach_awaiter final : promise_base::cancel_hook {
            promise_base *m_waiter;
            ref_counted_resource_handle m_task; // a second reference; the waiter keeps its own for the result
            std::shared_ptr<detach_link> m_link = std::make_shared<detach_link>();

            detach_awaiter(promise_base *waiter, ref_counted_resource_handle task) noexcept
                : m_waiter(waiter), m_task(std::move(task)) {
            }

            [[nodiscard]] constexpr static bool await_ready() noexcept {
                return false;
            }

            void await_suspend(std::coroutine_handle<>) {
                execution_context *context = m_waiter->get_execution_context();
                assert(context);
                auto *task = static_cast<promise_base *>(m_task.get());
                if (!task->get_execution_context()) {
                    task->set_execution_context(context);
                }

                m_link->waiter = m_waiter->weak_borrow();
                m_waiter->link_cancel_hook(this);

                // Like co_spawn: the task pins itself and keeps the context alive until it finishes. It has
                // no owner; its frame is on the heap (detach's parameter is not elidable).
                task->set_continuation([context_lock = context->lock_this(), self = task->borrow(),
                                        link = m_link] {
                    ref_counted_resource_weak_handle waiter;
                    {
                        std::lock_guard guard(link->mutex);
                        waiter = std::move(link->waiter);
                    }
                    if (!waiter.is_null()) {
                        schedule_resume(std::move(waiter));
                    }
                });
                context->resume_promise_strong(std::move(m_task));
                // Hand-off: the task may already have finished and resumed us.
            }

            void await_resume() noexcept {
                m_waiter->unlink_cancel_hook(this);
            }

            void on_cancel() noexcept override {
                std::lock_guard guard(m_link->mutex);
                m_link->waiter.reset(); // the task no longer refers to us; it keeps running
            }
        };

        struct detach_start : abstract_awaitable_base {
            ref_counted_resource_handle m_task;

            detach_awaiter by_promise(promise_base *waiter) && {
                return {waiter, std::move(m_task)};
            }
        };

        // Runs `task` in the background, like co_spawn, and returns a cancelable task for its result.
        // Cancelling (or dropping) the returned task does not cancel `task` and does not wait for it: use
        // this for blocking work that must not hold up an any_of / interrupt_by. The context's destructor
        // still waits for `task` to finish.
        template<template<typename> typename TaskType, typename T>
        cancelable_task<T> detach(TaskType<T> task) {
            co_await detach_start{{}, task.get_coroutine_handle()};
            co_return std::move(task).get_result();
        }

        // -----------------------------------------------------------------------------------------
        // multithreaded_execution_context
        // -----------------------------------------------------------------------------------------

        // Resumes coroutines on a thread pool; tasks hinted with e_io go to a separate pool so that
        // blocking work does not starve the compute threads.
        class multithreaded_execution_context : public execution_context {
        public:
            explicit multithreaded_execution_context(
                uint32_t thread_count = std::jthread::hardware_concurrency(),
                uint32_t io_thread_count = std::max(4u, 2 * std::jthread::hardware_concurrency()))
                : m_thread_pool(std::make_unique<dp_thread_pool::thread_pool<>>(
                      thread_count, [this](std::size_t) { t_worker = {this, false}; })),
                  m_io_thread_pool(std::make_unique<dp_thread_pool::thread_pool<>>(
                      io_thread_count, [this](std::size_t) { t_worker = {this, true}; })),
                  m_timer_thread([this](std::stop_token stop) { timer_loop(stop); }) {
            }

            // Waits for every spawned coroutine, then for everything they left behind (e.g. pinned
            // tasks that are still finishing after their parent was cancelled, pending timers). Only
            // then are the timer thread and the pools destroyed, so nothing can enqueue into a pool that
            // is already gone.
            ~multithreaded_execution_context() override {
                {
                    std::unique_lock lock(m_holders_mutex);
                    m_holders_changed.wait(lock, [this] { return m_holders == 0; });
                }
                wait_idle();
            }

            void resume_promise_weak(ref_counted_resource_weak_handle handle) override {
                const auto hint = static_cast<promise_base *>(handle.get_raw())->get_task_hint();

                // Fast path: called on one of our workers that runs coroutines with the same hint, and
                // nothing is queued for this thread yet. Instead of going through the pool, let the run
                // loop below resume the coroutine right after the current one has suspended or finished.
                if (t_worker.context == this && t_worker.io == static_cast<bool>(hint & promise_base::e_io) &&
                    t_next.is_null()) {
                    t_next = std::move(handle);
                    return;
                }

                enqueue(hint, [handle = std::move(handle)]() mutable {
                    // The coroutine may have been cancelled since it was scheduled. Keep this a separate
                    // statement: the weak reference must be gone before the coroutine runs. If it lived on
                    // until run_on_this_thread returned, it would keep a finished child's frame alive after
                    // its parent was resumed, and the parent could build its next child in the same HALO
                    // slot while we still hold a reference into it.
                    auto strong = std::exchange(handle, {}).lock();
                    run_on_this_thread(std::move(strong));
                });
            }

            void resume_promise_strong(ref_counted_resource_handle handle) override {
                const auto hint = static_cast<promise_base *>(handle.get())->get_task_hint();
                enqueue(hint, [handle = std::move(handle)]() mutable {
                    run_on_this_thread(std::move(handle));
                });
            }

            void wait_idle() override {
                for (auto n = m_pending_jobs.load(std::memory_order_acquire); n != 0;
                     n = m_pending_jobs.load(std::memory_order_acquire)) {
                    m_pending_jobs.wait(n, std::memory_order_acquire);
                }
            }

            // A count rather than a std::shared_mutex: with a reader/writer lock, a coroutine that calls
            // co_spawn (lock()) while the destructor is waiting for the exclusive lock can be blocked
            // behind it, and the destructor waits for that very coroutine: deadlock. lock() never blocks.
            void lock() override {
                std::lock_guard guard(m_holders_mutex);
                ++m_holders;
            }

            void unlock() override {
                // Notify while holding the mutex: the destructor cannot return from its wait (and destroy
                // the mutex and condition variable) before we have released it.
                std::lock_guard guard(m_holders_mutex);
                if (--m_holders == 0) {
                    m_holders_changed.notify_all();
                }
            }

            void add_timer(std::chrono::steady_clock::time_point deadline,
                           ref_counted_resource_weak_handle coroutine, timer_id &id) override {
                m_pending_jobs.fetch_add(1, std::memory_order_relaxed); // a pending timer is pending work
                std::lock_guard guard(m_timer_mutex);
                id = {deadline, m_next_timer_sequence++};
                const bool earliest = m_timers.empty() || id < m_timers.begin()->first;
                m_timers.emplace(id, std::move(coroutine));
                if (earliest) {
                    m_timer_changed.notify_one();
                }
            }

            void cancel_timer(const timer_id &id) noexcept override {
                ref_counted_resource_weak_handle coroutine;
                {
                    std::lock_guard guard(m_timer_mutex);
                    const auto it = m_timers.find(id);
                    if (it == m_timers.end()) {
                        return; // already fired; the resumption will find the coroutine cancelled
                    }
                    coroutine = std::move(it->second);
                    m_timers.erase(it);
                }
                coroutine.reset();
                job_finished();
            }

            void co_spawn(task_base<void> coroutine) override {
                promise_base *promise = coroutine.get_promise();
                assert(promise);
                promise->set_execution_context(this);
                promise->set_cancelable(false);

                // The continuation keeps both the context and the coroutine alive until it finishes.
                promise->set_continuation([context_lock = lock_this(), self = promise->borrow()]() {
                });

                resume_promise_strong(std::move(coroutine).release_handle());
            }

        private:
            // Which context and pool the current thread works for (set by the pools' init function).
            struct worker_identity {
                multithreaded_execution_context *context;
                bool io;
            };

            inline static thread_local worker_identity t_worker{nullptr, false};

            // The coroutine to resume next on this thread (see resume_promise_weak). Weak, like a queued
            // job: if it is cancelled before we get to it, lock() fails and it does not run.
            inline static thread_local ref_counted_resource_weak_handle t_next{};

            // Resumes `strong`, then whatever it scheduled into t_next, and so on. A loop rather than
            // nested resume() calls, so the stack depth stays constant however long the chain is. Each
            // coroutine is resumed only after resume_and_finish has returned for the previous one, i.e.
            // after the previous one released its references (see final_awaiter).
            //
            // t_next is checked even when there is nothing to resume: releasing a reference (e.g. the weak
            // one of a job whose coroutine was cancelled) can destroy a coroutine, and that can schedule
            // another one (an any_of waiting for its children to be gone) into t_next.
            static void run_on_this_thread(ref_counted_resource_handle strong) {
                for (;;) {
                    if (strong) {
                        promise_base::resume_and_finish(std::move(strong));
                    }
                    if (t_next.is_null()) {
                        return;
                    }
                    strong = std::exchange(t_next, {}).lock();
                }
            }

            template<typename Job>
            void enqueue(promise_base::task_hint hint, Job job) {
                m_pending_jobs.fetch_add(1, std::memory_order_relaxed);
                auto &pool = (hint & promise_base::e_io) ? *m_io_thread_pool : *m_thread_pool;
                pool.enqueue_detach([this, job = std::move(job)]() mutable {
                    struct job_done {
                        multithreaded_execution_context *self;

                        ~job_done() {
                            self->job_finished();
                        }
                    } done{this};
                    std::move(job)();
                });
            }

            // Jobs and timers started by a job were counted before the job itself finishes, so the count
            // only reaches zero once everything has finished.
            void job_finished() noexcept {
                if (m_pending_jobs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    m_pending_jobs.notify_all();
                }
            }

            // Fires timers in deadline order. Sleeps until the earliest deadline, or until an earlier timer
            // is added or the earliest one is cancelled.
            void timer_loop(std::stop_token stop) {
                std::unique_lock lock(m_timer_mutex);
                while (!stop.stop_requested()) {
                    if (m_timers.empty()) {
                        m_timer_changed.wait(lock, stop, [this] { return !m_timers.empty(); });
                        continue;
                    }
                    const timer_id earliest = m_timers.begin()->first;
                    if (std::chrono::steady_clock::now() < earliest.deadline) {
                        m_timer_changed.wait_until(lock, stop, earliest.deadline, [this, earliest] {
                            return m_timers.empty() || m_timers.begin()->first != earliest;
                        });
                        continue;
                    }
                    auto node = m_timers.extract(m_timers.begin());
                    lock.unlock();
                    resume_promise_weak(std::move(node.mapped())); // counted as a job of its own
                    job_finished(); // the timer itself
                    lock.lock();
                }
            }

            // Declared first so it is destroyed last: a worker may still be inside notify_all() when
            // wait_idle() returns, and the pools join their workers when they are destroyed.
            std::atomic_size_t m_pending_jobs{0};
            std::unique_ptr<dp_thread_pool::thread_pool<>> m_thread_pool;
            std::unique_ptr<dp_thread_pool::thread_pool<>> m_io_thread_pool;
            std::mutex m_holders_mutex;
            std::condition_variable m_holders_changed;
            std::size_t m_holders = 0; // co_spawn'ed roots that have not finished, plus explicit lock()s

            std::mutex m_timer_mutex;
            std::condition_variable_any m_timer_changed;
            std::map<timer_id, ref_counted_resource_weak_handle> m_timers;
            std::uint64_t m_next_timer_sequence = 0;
            // Declared last so it is destroyed (stopped and joined) first, while the pools still exist.
            std::jthread m_timer_thread;
        };
    }

    template<template<typename...> typename TaskType, typename T, typename... Args>
    T execution_context::block_on(TaskType<T, Args...> task) {
        auto *promise = task.get_promise();
        assert(promise);

        // std::promise / std::future share a reference-counted state, so the worker that calls
        // set_value() never touches memory that this function has already released.
        std::promise<void> finished;
        auto finished_future = finished.get_future();
        promise->set_continuation([finished = std::move(finished)]() mutable {
            finished.set_value();
        });
        promise->set_execution_context(this);
        promise->set_task_hint(_details::promise_base::e_none);

        resume_promise(promise);
        finished_future.wait();

        return std::move(task).get_result();
    }

    template<template<typename...> typename TaskType, typename T, typename... Args>
    std::future<T> execution_context::async_execute(TaskType<T, Args...> task) {
        lock(); // released by the async job; keeps the context alive until block_on returns
        return std::async(std::launch::async, [this, task = std::move(task)]() mutable {
            struct unlock_guard {
                execution_context *ctx;

                ~unlock_guard() {
                    ctx->unlock();
                }
            } unlock{this};

            return this->block_on(std::move(task));
        });
    }

    template<typename T>
    using task = _details::task<T>;

    template<typename T>
    using cancelable_task = _details::cancelable_task<T>;

    namespace _details {
        // -----------------------------------------------------------------------------------------
        // Building blocks for the combinators
        // -----------------------------------------------------------------------------------------

        template<typename T>
        using result_or_exception = std::expected<T, std::exception_ptr>;

        template<typename T>
        using non_throw_task = task_base<result_or_exception<T>>;

        // Wraps a task so that it completes with an std::expected instead of throwing.
        template<template<typename...> typename TaskType, typename T, typename... Rest>
        TaskType<result_or_exception<T>, Rest...> into_non_throw(COROUTINE_AWAIT_ELIDABLE_ARGUMENT TaskType<T, Rest...> _task) {
            auto task = std::move(_task);
            try {
                if constexpr (std::is_void_v<T>) {
                    co_await std::move(task);
                    co_return result_or_exception<void>{};
                } else {
                    co_return result_or_exception<T>{co_await std::move(task)};
                }
            } catch (...) {
                co_return std::unexpected{std::current_exception()};
            }
        }

        template<typename T, typename S>
        class ex_promise;

        // A task whose promise carries extra state `S` (used by all_of / any_of).
        template<typename T, typename S>
        class COROUTINE_AWAIT_ELIDABLE ex_task : public task_base<T> {
        public:
            using promise_type = ex_promise<T, S>;
            using typename task_base<T>::value_type;

            ex_task() = default;

            ex_task(ref_counted_resource_handle &&coroutine_handle) noexcept
                : task_base<T>(std::move(coroutine_handle)) {
            }

            ex_promise<T, S> *get_ex_promise() const {
                return static_cast<ex_promise<T, S> *>(this->get_promise_base());
            }
        };

        template<typename T, typename S>
        class ex_promise : public promise<T> {
        public:
            S state;

        protected:
            // `S` may provide release_resources() to drop what it owns on cancellation.
            void release_resources() noexcept override {
                if constexpr (requires { state.release_resources(); }) {
                    state.release_resources();
                }
                promise<T>::release_resources();
            }
        };

        // Awaiter that runs `callback(handle)` once the coroutine is suspended. The callback typically
        // starts work that will resume the coroutine later.
        template<typename Func> requires std::is_invocable_v<Func, std::coroutine_handle<>>
        struct suspend_and_then_t : std::suspend_always {
            Func m_callback;

            void await_suspend(std::coroutine_handle<> handle) noexcept {
                // Move the callback onto this thread's stack first: once it has started the work, the
                // coroutine may be resumed elsewhere and this awaiter (which lives in the coroutine
                // frame) destroyed while the callback is still running.
                Func callback = std::move(m_callback);
                std::move(callback)(handle);
                // Hand-off: do not touch `this` here.
            }
        };

        template<typename Func>
        suspend_and_then_t<std::decay_t<Func>> suspend_and_then(Func &&callback) {
            return {{}, std::forward<Func>(callback)};
        }

        // A coroutine that runs to completion immediately and hands back its argument. Awaiting
        // `force_await_embed_in_current_frame(make_task(...))` puts make_task's frame into the current
        // coroutine's frame via HALO, even though the task is not awaited right away.
        //
        // Do NOT use it inside a loop: the elided frame lives in a slot of the enclosing scope that is
        // reused by every iteration, so all tasks created in the loop would share one frame.
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

                static void unhandled_exception() noexcept {
                    std::terminate();
                }
            };

            std::coroutine_handle<promise_type> coro;

            immediate_value_task(std::coroutine_handle<promise_type> h) : coro(h) {
            }

            immediate_value_task(const immediate_value_task &) = delete;

            immediate_value_task &operator=(const immediate_value_task &) = delete;

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
        immediate_value_task<Task> force_await_embed_in_current_frame(COROUTINE_AWAIT_ELIDABLE_ARGUMENT Task task) {
            co_return task;
        }

        template<std::size_t N, typename Fn>
        void for_each_index(Fn &&fn) {
            [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                (fn(std::integral_constant<std::size_t, Is>{}), ...);
            }(std::make_index_sequence<N>{});
        }

        template<typename T>
        promise_base *promise_of(const task_base<T> &task) {
            return task.get_promise();
        }

        inline promise_base *promise_of(promise_base *promise) {
            return promise;
        }

        // The meeting point between a combinator (all_of / any_of) and its children. The children's
        // continuations and on_destroyed hooks refer to this small heap block, never to the combinator's
        // frame: a child that is still finishing its continuation after the combinator has been woken
        // would otherwise keep the combinator's frame alive while the combinator finishes and its caller
        // reuses its HALO slot. Only `waiter` refers to the combinator. It is set by the combinator just
        // before it suspends and moved out (handed to the scheduler, see schedule_resume) by whoever
        // wakes it, so once the combinator runs again nothing refers to it from here.
        struct combinator_sync {
            std::atomic_size_t pending{0}; // all_of: children not finished yet
            std::atomic_bool finished{false}; // any_of: the winner has been chosen
            std::size_t winner = 0; // any_of: published to the combinator by its resumption
            std::atomic_size_t alive{0}; // children not destroyed yet + 1, see wait_children_destroyed
            ref_counted_resource_weak_handle waiter;

            void wake() {
                schedule_resume(std::move(waiter));
            }
        };

        // Makes every element of `children` a child of `parent` and starts them. Called from the
        // parent's await_suspend. `make_continuation(index)` returns the continuation that child `index`
        // calls when it finishes; one of them eventually wakes the parent.
        //
        // The children are started in a second pass, after a weak reference to each has been taken.
        // Once the first child starts, the parent may be resumed on another thread (any_of resumes on the
        // first finished child) and release children that we have not started yet. Thanks to the weak
        // references their memory stays valid; their scheduled resumption then fails to lock() and they
        // never run, which is exactly the any_of cancellation semantics. The second pass holds no
        // reference to the parent at all.
        template<typename Children, typename MakeContinuation>
        void start_children(promise_base *parent, Children &children, MakeContinuation &&make_continuation) {
            execution_context *context = parent->get_execution_context();
            assert(context);

            using weak_handle = ref_counted_resource_weak_handle;
            using started_type = decltype([] {
                if constexpr (requires { std::tuple_size<Children>::value; }) {
                    return std::array<weak_handle, std::tuple_size_v<Children>>{};
                } else {
                    return std::vector<weak_handle>{};
                }
            }());
            started_type started{};
            if constexpr (requires { started.resize(0); }) {
                started.resize(std::size(children));
            }

            std::size_t index = 0;
            for (auto &child_ref: children) {
                promise_base *child = promise_of(child_ref);
                if (!child->get_execution_context()) {
                    child->set_execution_context(context);
                }
                child->set_owner(parent->weak_borrow());
                child->set_continuation(make_continuation(index));
                started[index] = child->weak_borrow();
                ++index;
            }

            // Hand-off starts with the first iteration: from here on only touch `started` and `context`.
            for (auto &child: started) {
                context->resume_promise_weak(std::move(child));
            }
        }

        // all_of: the last child to finish wakes the combinator.
        inline auto all_of_continuation(std::shared_ptr<combinator_sync> sync) {
            return [sync = std::move(sync)] {
                if (sync->pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    sync->wake();
                }
            };
        }

        // any_of: the first child to finish records its index and wakes the combinator.
        inline auto any_of_continuation(std::size_t index, std::shared_ptr<combinator_sync> sync) {
            return [index, sync = std::move(sync)] {
                if (!sync->finished.exchange(true, std::memory_order_acq_rel)) {
                    sync->winner = index;
                    sync->wake();
                }
            };
        }

        // Structured combinators: once all_of has its results (any_of its winner), it releases its children
        // and then waits until every one of them has been destroyed, together with everything below it (a
        // child is only destroyed after its own children, which hold it as their owner). Nothing a
        // combinator started outlives it, so all of it may live in HALO slots of the caller.
        //
        // `alive` starts at (number of children + 1): each child's on_destroyed hook takes one, and the
        // combinator takes the last one when it suspends here. Whoever takes the last one wakes it, so a
        // child destroyed before the combinator gets here (e.g. while it releases them) cannot be missed.
        inline auto on_child_destroyed(std::shared_ptr<combinator_sync> sync) {
            return [sync = std::move(sync)] {
                if (sync->alive.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    sync->wake();
                }
            };
        }

        struct wait_children_destroyed {
            combinator_sync *sync;
            promise_base *self;

            [[nodiscard]] constexpr static bool await_ready() noexcept {
                return false;
            }

            // Returning false resumes us at once: every child was already gone.
            [[nodiscard]] bool await_suspend(std::coroutine_handle<>) const noexcept {
                sync->waiter = self->weak_borrow();
                if (sync->alive.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    sync->waiter.reset();
                    return false;
                }
                return true; // hand-off: the last hook may already be waking us
            }

            constexpr static void await_resume() noexcept {
            }
        };

        // Starts the children of an any_of (tuple or vector of tasks) with its sync block.
        template<typename Children>
        void start_any_of_children(promise_base *promise, Children &children,
                                   const std::shared_ptr<combinator_sync> &sync) {
            sync->waiter = promise->weak_borrow(); // woken by the winner
            for (auto &child: children) {
                promise_of(child)->set_on_destroyed(on_child_destroyed(sync));
            }
            start_children(promise, children, [&sync](std::size_t index) {
                return any_of_continuation(index, sync);
            });
        }

        // Starts the children of an all_of (tuple or vector of tasks) with its sync block.
        template<typename Children>
        void start_all_of_children(promise_base *promise, Children &children,
                                   const std::shared_ptr<combinator_sync> &sync) {
            sync->pending.store(std::size(children), std::memory_order_relaxed);
            sync->waiter = promise->weak_borrow(); // woken by the last child to finish
            for (auto &child: children) {
                promise_of(child)->set_on_destroyed(on_child_destroyed(sync));
            }
            start_children(promise, children, [&sync](std::size_t) {
                return all_of_continuation(sync);
            });
        }

        // -----------------------------------------------------------------------------------------
        // all_of / any_of over a parameter pack
        //
        // Each child is wrapped by into_non_throw, so the results are std::expected and one failing
        // child does not affect the others. The wrappers are embedded in this coroutine's frame
        // (HALO) via force_await_embed_in_current_frame.
        // -----------------------------------------------------------------------------------------

        template<typename... T>
        struct all_of_args_shared_state {
            std::shared_ptr<combinator_sync> sync = std::make_shared<combinator_sync>();
            std::tuple<non_throw_task<T>...> children;

            // Called (through ex_promise) when the combinator is cancelled while its children run: release
            // them, which cancels the cancelable ones, and let go of the sync block, whose `waiter` may
            // refer to us. Without this the children, which hold our frame weakly, and the frame, which
            // holds them strongly here, would keep each other alive forever.
            void release_resources() noexcept {
                children = decltype(children){};
                sync.reset();
            }
        };

        // Each child's execution context, read *before* the children are moved from. (Reading it in
        // the same expression as the move is not safe: the order of evaluation inside
        // `x.with_context(y)` is not something we can rely on across compilers.)
        template<typename... Tasks>
        std::array<execution_context *, sizeof...(Tasks)> contexts_of(const Tasks &... tasks) {
            return {tasks.get_promise()->get_execution_context()...};
        }

        template<typename Tuple, std::size_t N>
        void assign_contexts(Tuple &children, const std::array<execution_context *, N> &contexts) {
            for_each_index<N>([&](auto i) {
                std::get<decltype(i)::value>(children).get_promise()->set_execution_context(contexts[i]);
            });
        }

        template<typename... Tasks>
        std::array<promise_base *, sizeof...(Tasks)> promises_of(std::tuple<Tasks...> &children) {
            return std::apply([](auto &... child) {
                return std::array<promise_base *, sizeof...(Tasks)>{child.get_promise()...};
            }, children);
        }

        // Waits for all tasks. Never throws; each element holds the value or the exception of one task.
        template<typename... Tasks> requires (sizeof...(Tasks) > 0)
        ex_task<std::tuple<result_or_exception<typename Tasks::value_type>...>,
            all_of_args_shared_state<typename Tasks::value_type...>>
        all_of(COROUTINE_AWAIT_ELIDABLE_ARGUMENT Tasks... tasks) {
            using result_type = std::tuple<result_or_exception<typename Tasks::value_type>...>;
            using state_type = all_of_args_shared_state<typename Tasks::value_type...>;
            using promise_type = ex_promise<result_type, state_type>;

            auto *promise = static_cast<promise_type *>(co_await get_current_promise());
            state_type *state = &promise->state;

            const auto contexts = contexts_of(tasks...);
            state->children = std::make_tuple(
                co_await force_await_embed_in_current_frame(into_non_throw(std::move(tasks)))...);
            assign_contexts(state->children, contexts);

            state->sync->alive.store(sizeof...(Tasks) + 1, std::memory_order_relaxed);

            co_await suspend_and_then([promise](std::coroutine_handle<>) {
                auto children = promises_of(promise->state.children);
                start_all_of_children(promise, children, promise->state.sync);
            });

            // Every child has finished.
            result_type results = std::apply([](auto &... child) {
                return result_type{child.get_promise()->get_result_move()...};
            }, state->children);
            state->children = {};
            co_await wait_children_destroyed{state->sync.get(), promise};
            co_return results;
        }

        template<typename... T>
        struct any_of_args_shared_state {
            std::shared_ptr<combinator_sync> sync = std::make_shared<combinator_sync>();
            std::tuple<non_throw_task<T>...> children;

            // Called (through ex_promise) when any_of is cancelled while its children run: release them,
            // which cancels the cancelable ones, and let go of the sync block, whose `waiter` may refer
            // to us (otherwise: frame -> sync -> frame).
            void release_resources() noexcept {
                children = decltype(children){};
                sync.reset();
            }
        };

        // Moves out the winner's result and releases every child (cancelable losers stop at their next
        // suspension point, the others run on).
        template<typename Result, typename State>
        Result take_any_of_result(State &state) {
            Result results{};
            for_each_index<std::tuple_size_v<Result>>([&](auto i) {
                constexpr std::size_t index = decltype(i)::value;
                if (index == state.sync->winner) {
                    std::get<index>(results) = std::get<index>(state.children).get_promise()->get_result_move();
                }
            });
            state.children = {};
            return results;
        }

        template<typename... Tasks>
        using any_of_args_result = std::tuple<std::optional<result_or_exception<typename Tasks::value_type>>...>;

        // Waits for the first task to finish; exactly one element of the result is engaged. The other
        // tasks are released: cancelable ones stop at their next suspension point, non-cancelable ones
        // run to completion. any_of returns only once all of them are gone (structured, see
        // wait_children_destroyed), so everything here may be embedded in the caller's frame (HALO),
        // loops included. To not wait for a slow, non-cancelable loser, pass it through detach().
        template<typename... Tasks> requires (sizeof...(Tasks) > 0)
        ex_task<any_of_args_result<Tasks...>, any_of_args_shared_state<typename Tasks::value_type...>>
        any_of(COROUTINE_AWAIT_ELIDABLE_ARGUMENT Tasks... tasks) {
            using state_type = any_of_args_shared_state<typename Tasks::value_type...>;
            using promise_type = ex_promise<any_of_args_result<Tasks...>, state_type>;

            auto *promise = static_cast<promise_type *>(co_await get_current_promise());
            state_type *state = &promise->state;

            const auto contexts = contexts_of(tasks...);
            state->children = std::make_tuple(
                co_await force_await_embed_in_current_frame(into_non_throw(std::move(tasks)))...);
            assign_contexts(state->children, contexts);
            state->sync->alive.store(sizeof...(Tasks) + 1, std::memory_order_relaxed);

            co_await suspend_and_then([promise](std::coroutine_handle<>) {
                auto children = promises_of(promise->state.children);
                start_any_of_children(promise, children, promise->state.sync);
            });

            auto results = take_any_of_result<any_of_args_result<Tasks...>>(*state);
            co_await wait_children_destroyed{state->sync.get(), promise};
            co_return results;
        }

        // -----------------------------------------------------------------------------------------
        // all_of / any_of over a vector
        //
        // No HALO for the wrappers: they are created in a loop (see immediate_value_task), so each one
        // gets its own heap frame.
        // -----------------------------------------------------------------------------------------

        template<typename Task>
        std::vector<non_throw_task<typename Task::value_type>> wrap_non_throw(std::vector<Task> &tasks) {
            std::vector<non_throw_task<typename Task::value_type>> wrapped;
            wrapped.reserve(tasks.size());
            for (auto &task: tasks) {
                execution_context *context = task.get_promise()->get_execution_context();
                auto wrapper = into_non_throw(std::move(task));
                wrapper.get_promise()->set_execution_context(context);
                wrapped.push_back(std::move(wrapper));
            }
            tasks.clear();
            return wrapped;
        }

        template<typename T>
        struct all_of_vector_shared_state {
            std::shared_ptr<combinator_sync> sync = std::make_shared<combinator_sync>();
            std::vector<non_throw_task<T>> children;

            // See all_of_args_shared_state.
            void release_resources() noexcept {
                children = decltype(children){};
                sync.reset();
            }
        };

        template<typename Task>
        ex_task<std::vector<result_or_exception<typename Task::value_type>>,
            all_of_vector_shared_state<typename Task::value_type>>
        all_of(COROUTINE_AWAIT_ELIDABLE_ARGUMENT std::vector<Task> tasks) {
            using T = typename Task::value_type;
            using result_type = std::vector<result_or_exception<T>>;
            using state_type = all_of_vector_shared_state<T>;
            using promise_type = ex_promise<result_type, state_type>;

            auto *promise = static_cast<promise_type *>(co_await get_current_promise());
            state_type *state = &promise->state;

            state->children = wrap_non_throw(tasks);
            if (state->children.empty()) {
                co_return result_type{}; // nothing would ever resume us
            }

            state->sync->alive.store(state->children.size() + 1, std::memory_order_relaxed);

            co_await suspend_and_then([promise](std::coroutine_handle<>) {
                start_all_of_children(promise, promise->state.children, promise->state.sync);
            });

            result_type results;
            results.reserve(state->children.size());
            for (auto &child: state->children) {
                results.push_back(child.get_promise()->get_result_move());
            }
            state->children.clear();
            co_await wait_children_destroyed{state->sync.get(), promise};
            co_return results;
        }

        template<typename T>
        struct any_of_vector_shared_state {
            std::shared_ptr<combinator_sync> sync = std::make_shared<combinator_sync>();
            std::vector<non_throw_task<T>> children;

            // See any_of_args_shared_state.
            void release_resources() noexcept {
                children = decltype(children){};
                sync.reset();
            }
        };

        // Vector version of any_of; also structured (returns once every child is gone).
        template<typename Task>
        ex_task<std::vector<std::optional<result_or_exception<typename Task::value_type>>>,
            any_of_vector_shared_state<typename Task::value_type>>
        any_of(COROUTINE_AWAIT_ELIDABLE_ARGUMENT std::vector<Task> tasks) {
            using T = typename Task::value_type;
            using result_type = std::vector<std::optional<result_or_exception<T>>>;
            using state_type = any_of_vector_shared_state<T>;
            using promise_type = ex_promise<result_type, state_type>;

            auto *promise = static_cast<promise_type *>(co_await get_current_promise());
            state_type *state = &promise->state;

            state->children = wrap_non_throw(tasks);
            if (state->children.empty()) {
                co_return result_type{}; // nothing would ever resume us
            }
            state->sync->alive.store(state->children.size() + 1, std::memory_order_relaxed);

            co_await suspend_and_then([promise](std::coroutine_handle<>) {
                start_any_of_children(promise, promise->state.children, promise->state.sync);
            });

            const std::size_t winner = state->sync->winner;
            result_type results(state->children.size());
            results[winner] = state->children[winner].get_promise()->get_result_move();
            state->children.clear(); // releases every child; the cancelable losers stop
            co_await wait_children_destroyed{state->sync.get(), promise};
            co_return results;
        }

        // -----------------------------------------------------------------------------------------
        // Asynchronous generator
        // -----------------------------------------------------------------------------------------

        template<typename T> requires(!std::same_as<T, void>)
        class generator_promise : public promise_base {
        public:
            // The last yielded value, or std::nullopt once the generator has finished.
            std::optional<T> get_result_move() {
                return std::visit(overloaded{
                                      [](std::monostate &) -> std::optional<T> {
                                          return std::nullopt;
                                      },
                                      [](T &value) -> std::optional<T> {
                                          return std::move(value);
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

            // Suspends the generator and resumes the consumer.
            struct yield_awaiter : std::suspend_always {
                promise_base *m_promise;

                void await_suspend(std::coroutine_handle<>) const noexcept {
                    if (auto continuation = m_promise->take_continuation()) {
                        continuation();
                    }
                    // Hand-off: the consumer may already be resuming us again.
                }
            };

            template<std::convertible_to<T> U>
            yield_awaiter yield_value(U &&value) {
                m_current_value.template emplace<T>(std::forward<U>(value));
                return {{}, this};
            }

            [[nodiscard]] bool is_done() const noexcept {
                return this->get_coroutine_handle().done();
            }

        private:
            std::variant<std::monostate, T, std::exception_ptr> m_current_value{std::monostate{}};
        };

        // `while (auto value = co_await gen) ...` yields std::optional<T>; std::nullopt means done.
        template<typename T> requires(!std::same_as<T, void>)
        class generator : public future_base, public abstract_awaitable_base, public std::suspend_always {
        public:
            using promise_type = generator_promise<T>;

            generator(ref_counted_resource_handle coroutine_handle) noexcept
                : future_base(std::move(coroutine_handle)) {
                assert(get_promise());
            }

            generator_promise<T> *get_promise() const {
                return static_cast<promise_type *>(this->get_promise_base());
            }

            template<typename Self>
            decltype(auto) with_context(this Self &&self, execution_context *ctx) {
                self.get_promise()->set_execution_context(ctx);
                return std::forward<Self>(self);
            }

            std::optional<T> await_resume() {
                assert(this->m_coroutine_handle);
                return get_promise()->get_result_move();
            }

            auto by_promise(promise_base *parent_promise) {
                assert(parent_promise);

                struct awaiter {
                    generator<T> *m_generator;
                    promise_base *m_parent_promise;

                    [[nodiscard]] bool await_ready() const noexcept {
                        return m_generator->get_promise()->is_done();
                    }

                    void await_suspend(std::coroutine_handle<>) const noexcept {
                        execution_context *context = m_parent_promise->get_execution_context();
                        assert(context);

                        promise_base *promise = m_generator->get_promise();
                        if (!promise->get_execution_context()) {
                            promise->set_execution_context(context);
                        }

                        auto weak_parent = m_parent_promise->weak_borrow();
                        promise->set_owner(weak_parent);
                        promise->set_continuation([weak_parent = std::move(weak_parent)]() mutable {
                            // Each co_await of the generator installs a fresh continuation, so it is used once.
                            schedule_resume(std::move(weak_parent));
                        });

                        promise->get_execution_context()->resume_promise(promise);
                        // Hand-off: do not touch `this` here.
                    }

                    std::optional<T> await_resume() const {
                        return m_generator->get_promise()->get_result_move();
                    }
                };

                return awaiter{this, parent_promise};
            }
        };

#if __has_include(<generator>)
        // Turns a synchronous std::generator into an asynchronous one.
        template<typename T>
        generator<T> asyncify(std::generator<T> gen) {
            for (auto &&value: gen) {
                co_yield value;
            }
        }
#endif

        // Turns a function returning an awaitable into a function returning TaskType.
        template<template<typename> typename TaskType = task, typename Fn>
        auto asyncify_function(Fn fn) {
            return [fn = std::move(fn)]<typename... Args>(this auto self, Args &&... args)
                -> TaskType<std::invoke_result_t<Fn, Args...>> {
                co_return co_await self.fn(std::forward<Args>(args)...);
            };
        }

        template<typename T>
        class COROUTINE_AWAIT_ELIDABLE io_task;

        // Waits for a std::future without blocking a thread (it is polled, see poll_until).
        template<template<typename> typename TaskType = cancelable_task, typename T>
        TaskType<T> from_future(std::future<T> future) {
            co_await poll_until([&future] {
                return future.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
            });
            co_return future.get();
        }
    }

    using _details::all_of;
    using _details::any_of;
    using _details::generator;
    using _details::asyncify;
    using _details::from_future;
    using _details::asyncify_function;
    using _details::get_current_coroutine_context;
    using _details::get_current_promise_base;
    using _details::get_current_coroutine_info;
    using _details::delay;
    using _details::poll_until;
    using _details::detach;

    template<typename T>
    using future = _details::task<T>;

    namespace _details {
        class interrupted_exception : public std::exception {
        public:
            explicit interrupted_exception(pin_resource_base *interrupted_by)
                : m_message(std::format("Task was interrupted by coroutine with handle: {:p}",
                                        static_cast<void *>(interrupted_by))) {
            }

            [[nodiscard]] const char *what() const noexcept override {
                return m_message.c_str();
            }

        private:
            std::string m_message;
        };

        // A task that can be raced against an "interrupt signal" task.
        template<typename T>
        class COROUTINE_AWAIT_ELIDABLE interruptable_task : public task_base<T> {
        public:
            interruptable_task() = default;

            interruptable_task(ref_counted_resource_handle &&coroutine_handle) noexcept
                : task_base<T>(std::move(coroutine_handle)) {
            }

            // Returns this task's result, or throws interrupted_exception if `interrupt_signal`
            // finishes first. Like any_of, it returns only once both tasks are gone, so a non-cancelable
            // task is not actually interrupted: make it cancelable, or detach() it.
            template<template<typename...> typename TaskType>
            task<T> interrupt_by(COROUTINE_AWAIT_ELIDABLE_ARGUMENT this interruptable_task<T> self,
                                 COROUTINE_AWAIT_ELIDABLE_ARGUMENT TaskType<void> interrupt_signal) {
                auto *signal = interrupt_signal.get_promise();
                auto [this_task, interrupt_task] = co_await any_of(std::move(self), std::move(interrupt_signal));
                if (this_task) {
                    if (!this_task->has_value()) {
                        std::rethrow_exception(this_task->error());
                    }
                    if constexpr (!std::is_void_v<T>) {
                        co_return std::move(this_task->value());
                    } else {
                        co_return;
                    }
                }
                throw interrupted_exception{static_cast<pin_resource_base *>(signal)};
            }

            // Like interrupt_by, but returns std::nullopt when interrupted and never throws.
            template<typename TaskType>
            task<std::optional<result_or_exception<T>>> interrupt_by_nothrow(
                COROUTINE_AWAIT_ELIDABLE_ARGUMENT this interruptable_task<T> self,
                COROUTINE_AWAIT_ELIDABLE_ARGUMENT TaskType interrupt_signal) {
                auto [this_task, interrupt_task] = co_await any_of(std::move(self), std::move(interrupt_signal));
                co_return std::move(this_task);
            }
        };

        // A task that runs on the I/O pool (for blocking work).
        template<typename T>
        class COROUTINE_AWAIT_ELIDABLE io_task : public task_base<T> {
        public:
            io_task() = default;

            io_task(ref_counted_resource_handle &&coroutine_handle) noexcept
                : task_base<T>(std::move(coroutine_handle)) {
                auto *promise = this->get_promise();
                promise->set_task_hint(static_cast<promise_base::task_hint>(promise->get_task_hint() | promise_base::e_io));
            }
        };

        // Acquires the semaphore without blocking a thread (it is polled, see poll_until).
        inline cancelable_task<void> wait_for_semaphore(std::shared_ptr<std::binary_semaphore> semaphore) {
            co_await poll_until([&semaphore] { return semaphore->try_acquire(); });
        }

        // Timer-based: no thread is blocked, and cancelling it (e.g. as an any_of loser) is immediate.
        inline cancelable_task<void> sleep_for(std::chrono::steady_clock::duration duration) {
            co_await delay(duration);
        }
    }

    using _details::interruptable_task;
    using _details::interrupted_exception;
    using _details::wait_for_semaphore;
    using _details::sleep_for;
    using _details::multithreaded_execution_context;
}
