#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/cast.h>

#include <atomic>
#include <chrono>
#include <future>
#include <vector>

using namespace NActors;

namespace {

using TClock = std::chrono::steady_clock;

struct TEvToken : TEventLocal<TEvToken, EventSpaceBegin(TEvents::ES_PRIVATE)> {
    ui64 Remaining;

    explicit TEvToken(ui64 remaining)
        : Remaining(remaining)
    {}
};

struct TResult {
    ui64 Expected = 0;
    // Only the executor accesses these fields until completion is published.
    ui64 Received = 0;
    ui64 Checksum = 0;
    TClock::time_point Begin;
    TClock::time_point End;
    std::atomic<bool> Done = false;
    std::promise<void> Completion;
};

class TTokenActor : public TActor<TTokenActor> {
private:
    TResult& Result_;

public:
    TActorId Next;

    explicit TTokenActor(TResult& result)
        : TActor(&TThis::StateWork)
        , Result_(result)
    {}

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() != TEvToken::EventType) {
            return;
        }
        const ui64 remaining = ev->Get<TEvToken>()->Remaining;
        if (!Result_.Received) {
            Result_.Begin = TClock::now();
        }
        ++Result_.Received;
        Result_.Checksum += remaining;
        if (remaining > 1) {
            Send(Next, new TEvToken(remaining - 1));
        }
        if (Result_.Received == Result_.Expected) {
            Result_.End = TClock::now();
            Result_.Done.store(true, std::memory_order_release);
            Result_.Completion.set_value();
        }
    }
};

template <class TRegister>
TActorId MakeRing(TResult& result, ui32 actors, TRegister&& registerActor) {
    std::vector<TTokenActor*> objects;
    std::vector<TActorId> ids;
    for (ui32 i = 0; i < actors; ++i) {
        auto* actor = new TTokenActor(result);
        objects.push_back(actor);
        ids.push_back(registerActor(actor));
    }
    for (ui32 i = 0; i < actors; ++i) {
        objects[i]->Next = ids[(i + 1) % actors];
    }
    return ids.front();
}

void RunProduction(TResult& result, ui32 actors, ui64 messages, ui32 window) {
    auto setup = MakeHolder<TActorSystemSetup>();
    setup->NodeId = 1;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0].Reset(new TBasicExecutorPool(0, 1, 0));
    setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig()));
    TActorSystem system(setup);
    system.Start();
    const auto first = MakeRing(result, actors, [&](IActor* actor) {
        return system.Register(actor, TMailboxType::Simple, 0);
    });
    auto done = result.Completion.get_future();
    for (ui32 i = 0; i < window; ++i) {
        system.Send(new IEventHandle(first, {}, new TEvToken(messages / window)));
    }
    const bool ready = done.wait_for(std::chrono::seconds(60)) == std::future_status::ready;
    if (!ready) {
        system.Stop();
        system.Cleanup();
        ythrow yexception() << "Production runtime timed out";
    }
    done.get();
    system.Stop();
    system.Cleanup();
}

void RunRuntime(TResult& result, ui32 actors, ui64 messages, ui32 window) {
    TTestActorRuntimeBase runtime;
    runtime.Initialize();
    runtime.SetDispatcherRandomSeed(TInstant::MicroSeconds(1), 0);
    runtime.SetDispatchedEventsLimit(messages + 10000);
    const auto first = MakeRing(result, actors, [&](IActor* actor) {
        return runtime.Register(actor);
    });
    for (ui32 i = 0; i < window; ++i) {
        runtime.Send(new IEventHandle(first, {}, new TEvToken(messages / window)), 0, true);
    }
    TDispatchOptions options;
    options.CustomFinalCondition = [&] { return result.Done.load(std::memory_order_acquire); };
    runtime.DispatchEvents(options);
}

void RunTestSystem(TResult& result, ui32 actors, ui64 messages, ui32 window) {
    NKikimr::TTestActorSystem system(1);
    system.Start();
    const auto first = MakeRing(result, actors, [&](IActor* actor) {
        return system.Register(actor, 1);
    });
    for (ui32 i = 0; i < window; ++i) {
        system.Send(new IEventHandle(first, {}, new TEvToken(messages / window)), 1);
    }
    system.Sim([&] { return !result.Done.load(std::memory_order_acquire); });
    system.Stop();
}

} // namespace

// Usage: actor_runtime_bench [all|production|runtime|test-system] [messages] [repetitions] [actors] [window]
// Initialization, registration, destruction and reporting are outside the timer.
// Each sample uses fresh runtimes; the first repetition is an unreported warmup.
int main(int argc, char** argv) {
    const TString mode = argc > 1 ? argv[1] : "all";
    const ui64 messages = argc > 2 ? FromString<ui64>(argv[2]) : 200000;
    const ui32 repetitions = argc > 3 ? FromString<ui32>(argv[3]) : 7;
    const ui32 actors = argc > 4 ? FromString<ui32>(argv[4]) : 2;
    const ui32 window = argc > 5 ? FromString<ui32>(argv[5]) : 1;
    Y_ENSURE(messages > 1 && messages <= 100000000 && actors && repetitions, "Invalid benchmark dimensions");
    Y_ENSURE(window && messages % window == 0, "Messages must be divisible by window");
    Y_ENSURE(mode == "all" || mode == "production" || mode == "runtime" || mode == "test-system", "Unknown runtime: " << mode);
    struct TRunner {
        const char* Name;
        void (*Run)(TResult&, ui32, ui64, ui32);
    };
    const TRunner runners[] = {
        {"production", RunProduction}, {"runtime", RunRuntime}, {"test-system", RunTestSystem}
    };
    Cout << "runtime,actors,window,messages,repetition,nanoseconds,ns_per_message" << Endl;
    for (ui32 repetition = 0; repetition <= repetitions; ++repetition) {
        // Rotate the runtime order to reduce systematic thermal/order effects.
        for (ui32 i = 0; i < 3; ++i) {
            const auto& runner = runners[(i + repetition) % 3];
            if (mode != "all" && mode != runner.Name) {
                continue;
            }
            TResult result;
            result.Expected = messages;
            runner.Run(result, actors, messages, window);
            Y_ENSURE(result.Done && result.Received == messages, runner.Name << ": incomplete delivery");
            const ui64 perChain = messages / window;
            Y_ENSURE(result.Checksum == window * perChain * (perChain + 1) / 2, runner.Name << ": checksum mismatch");
            const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(result.End - result.Begin).count();
            if (repetition) {
                Cout << runner.Name << ',' << actors << ',' << window << ',' << messages << ',' << repetition << ','
                     << ns << ',' << double(ns) / messages << Endl;
            }
        }
    }
}
