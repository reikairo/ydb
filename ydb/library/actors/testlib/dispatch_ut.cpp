#include "test_runtime.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace {

struct TEvValue : TEventLocal<TEvValue, EventSpaceBegin(TEvents::ES_PRIVATE)> {
    ui64 Value;

    explicit TEvValue(ui64 value)
        : Value(value)
    {}
};

class TRecordingActor : public TActor<TRecordingActor> {
public:
    TRecordingActor(TVector<ui64>& received, bool scheduleNext = false,
            std::function<void(ui64)> onReceive = {})
        : TActor(&TThis::StateWork)
        , Received_(received)
        , ScheduleNext_(scheduleNext)
        , OnReceive_(std::move(onReceive))
    {}

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() != TEvValue::EventType) {
            return;
        }
        const ui64 value = ev->Get<TEvValue>()->Value;
        Received_.push_back(value);
        if (ScheduleNext_ && value == 1) {
            Schedule(TDuration::MilliSeconds(10), new TEvValue(2));
        }
        if (OnReceive_) {
            OnReceive_(value);
        }
    }

private:
    TVector<ui64>& Received_;
    const bool ScheduleNext_;
    const std::function<void(ui64)> OnReceive_;
};

class TInspectRuntime : public TTestActorRuntimeBase {
public:
    TIntrusivePtr<TEventMailBox> RetainMailbox(const TActorId& actor) {
        return Mailboxes.at(TEventMailboxId(actor.NodeId(), actor.Hint()));
    }
};

void Queue(TTestActorRuntimeBase& runtime, const TActorId& actor, ui64 value) {
    runtime.Send(new IEventHandle(actor, {}, new TEvValue(value)), 0, true);
}

void DispatchUntil(TTestActorRuntimeBase& runtime, const TVector<ui64>& received, size_t count) {
    TDispatchOptions options;
    options.CustomFinalCondition = [&] { return received.size() >= count; };
    runtime.DispatchEvents(options, TDuration::Seconds(1));
    UNIT_ASSERT_VALUES_EQUAL(received.size(), count);
}

} // namespace

Y_UNIT_TEST_SUITE(TestRuntimeDispatch) {
    Y_UNIT_TEST(ScheduledEventSurvivesEmptyImmediateQueue) {
        TTestActorRuntimeBase runtime;
        runtime.SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto&&) { return false; });
        runtime.Initialize();
        TVector<ui64> received;
        const auto actor = runtime.Register(new TRecordingActor(received, true));
        runtime.EnableScheduleForActor(actor);
        Queue(runtime, actor, 1);
        DispatchUntil(runtime, received, 1);
        DispatchUntil(runtime, received, 2);
        UNIT_ASSERT_VALUES_EQUAL(received, (TVector<ui64>{1, 2}));
        UNIT_ASSERT(runtime.GetCurrentTime() >= TInstant::Zero() + TDuration::MilliSeconds(10));
    }

    Y_UNIT_TEST(ExpiredFreezeDoesNotLeakAfterClockRewind) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        TVector<ui64> received;
        const auto first = runtime.Register(new TRecordingActor(received));
        const auto second = runtime.Register(new TRecordingActor(received));
        // Drain bootstrap events before exercising an otherwise idle runtime.
        Queue(runtime, first, 0);
        DispatchUntil(runtime, received, 1);
        received.clear();
        bool frozen = false;
        runtime.SetReschedulingDelay(TDuration::MilliSeconds(10));
        auto previousObserver = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetRecipientRewrite() == first && !frozen) {
                frozen = true;
                return TTestActorRuntimeBase::EEventAction::RESCHEDULE;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
        Queue(runtime, first, 1);
        DispatchUntil(runtime, received, 1);
        UNIT_ASSERT(frozen);
        UNIT_ASSERT(runtime.GetCurrentTime() >= TInstant::Zero() + TDuration::MilliSeconds(10));
        runtime.SetObserverFunc(std::move(previousObserver));
        runtime.UpdateCurrentTime(TInstant::Zero(), true);
        Queue(runtime, second, 2);
        DispatchUntil(runtime, received, 2);
        UNIT_ASSERT_VALUES_EQUAL(received, (TVector<ui64>{1, 2}));
        UNIT_ASSERT_VALUES_EQUAL(runtime.GetCurrentTime(), TInstant::Zero());
    }

    Y_UNIT_TEST(RetainedEmptyMailboxDoesNotReceiveAnotherActorsEvents) {
        TInspectRuntime runtime;
        runtime.Initialize();
        TVector<ui64> received;
        const auto first = runtime.Register(new TRecordingActor(received));
        const auto second = runtime.Register(new TRecordingActor(received));
        Queue(runtime, first, 0);
        DispatchUntil(runtime, received, 1);
        received.clear();
        Queue(runtime, first, 1);
        auto retained = runtime.RetainMailbox(first);
        DispatchUntil(runtime, received, 1);
        UNIT_ASSERT(retained->IsEmpty());
        Queue(runtime, second, 2);
        UNIT_ASSERT(retained->IsEmpty());
        DispatchUntil(runtime, received, 2);
        UNIT_ASSERT_VALUES_EQUAL(received, (TVector<ui64>{1, 2}));
        UNIT_ASSERT(retained->IsEmpty());
    }

    Y_UNIT_TEST(NestedRestrictedDispatchPreservesOuterQueue) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        TVector<ui64> received;
        const auto second = runtime.Register(new TRecordingActor(received));
        const auto first = runtime.Register(new TRecordingActor(received, false, [&](ui64 value) {
            if (value == 1) {
                Queue(runtime, second, 2);
                TDispatchOptions nested;
                nested.OnlyMailboxes.emplace_back(second.NodeId(), second.Hint());
                nested.CustomFinalCondition = [&] { return received.size() == 2; };
                runtime.DispatchEvents(nested, TDuration::Seconds(1));
            }
        }));
        Queue(runtime, first, 1);
        Queue(runtime, first, 3);
        DispatchUntil(runtime, received, 3);
        UNIT_ASSERT_VALUES_EQUAL(received, (TVector<ui64>{1, 2, 3}));
    }
}

// Experimental scheduler checks appended to the existing dispatch suite.
#include <chrono>
#include <future>
#include <thread>

Y_UNIT_TEST_SUITE(TestRuntimeFastTimeExperiment) {
    Y_UNIT_TEST(QuietDoesNotConsumeTimers) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        TVector<ui64> received;
        const auto actor = runtime.Register(new TRecordingActor(received));
        runtime.Schedule(new IEventHandle(actor, {}, new TEvValue(2)), TDuration::Seconds(1));
        runtime.SetDispatchTimeout(TDuration::MilliSeconds(20));
        TDispatchOptions options;
        options.Quiet = true;
        options.OnlyMailboxes.emplace_back(actor.NodeId(), actor.Hint());
        options.CustomFinalCondition = [] { return false; };
        UNIT_ASSERT_EXCEPTION(runtime.DispatchEvents(options), TEmptyEventQueueException);
        UNIT_ASSERT(received.empty());
        UNIT_ASSERT_VALUES_EQUAL(runtime.GetCurrentTime(), TInstant::Zero());
        UNIT_ASSERT_VALUES_EQUAL(runtime.CaptureScheduledEvents().size(), 1);
    }

    Y_UNIT_TEST(IdleDispatcherAcceptsLateExternalSend) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        TVector<ui64> received;
        const auto actor = runtime.Register(new TRecordingActor(received));
        auto* system = runtime.GetActorSystem(0);
        std::promise<void> started;
        auto ready = started.get_future();
        auto sender = std::async(std::launch::async, [&] {
            if (ready.wait_for(std::chrono::seconds(2)) != std::future_status::ready) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            system->Send(new IEventHandle(actor, {}, new TEvValue(7)));
        });
        bool signalled = false;
        runtime.SetDispatchTimeout(TDuration::Seconds(2));
        TDispatchOptions options;
        options.OnlyMailboxes.emplace_back(actor.NodeId(), actor.Hint());
        options.CustomFinalCondition = [&] {
            if (!signalled) {
                signalled = true;
                started.set_value();
            }
            return !received.empty();
        };
        UNIT_ASSERT(runtime.DispatchEvents(options));
        sender.get();
        UNIT_ASSERT_VALUES_EQUAL(received, (TVector<ui64>{7}));
        UNIT_ASSERT_VALUES_EQUAL(runtime.GetCurrentTime(), TInstant::Zero());
    }

    Y_UNIT_TEST(ExternalSendProgressesDuringTimerChain) {
        TTestActorRuntimeBase runtime;
        runtime.SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto&&) { return false; });
        runtime.Initialize();
        runtime.SetScheduledLimit(1000000);
        TVector<ui64> received;
        std::promise<void> started;
        auto ready = started.get_future();
        bool delivered = false;
        TActorId actor;
        actor = runtime.Register(new TRecordingActor(received, false, [&](ui64 value) {
            if (value == 1) started.set_value();
            if (value == 999) {
                delivered = true;
            } else if (!delivered) {
                runtime.Schedule(new IEventHandle(actor, {}, new TEvValue(2)), TDuration::MilliSeconds(1));
            }
        }));
        runtime.EnableScheduleForActor(actor);
        auto* system = runtime.GetActorSystem(0);
        auto sender = std::async(std::launch::async, [&] {
            if (ready.wait_for(std::chrono::seconds(2)) != std::future_status::ready) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            system->Send(new IEventHandle(actor, {}, new TEvValue(999)));
        });
        Queue(runtime, actor, 1);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        TDispatchOptions options;
        options.CustomFinalCondition = [&] {
            return delivered || std::chrono::steady_clock::now() >= deadline;
        };
        runtime.DispatchEvents(options);
        sender.get();
        UNIT_ASSERT(delivered);
        UNIT_ASSERT(received.size() >= 2);
        UNIT_ASSERT_VALUES_EQUAL(received.front(), 1);
    }
}
