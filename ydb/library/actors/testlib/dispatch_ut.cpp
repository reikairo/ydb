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
