package org.fy.reactor;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class FluxTests {

    @Test
    void subscribe1() {
        Flux<Integer> range = Flux.range(1, 3);
        range.subscribe();
    }

    @Test
    void subscribe2() {
        Flux<Integer> range = Flux.range(1, 3);
        range.subscribe(integer -> log.info("integer : {}", integer));
    }

    @Test
    void subscribe3() {
        Flux<Integer> range = Flux.range(1, 4)
                .map(integer -> {
                    if (integer <= 3) return integer;
                    throw new RuntimeException("Got to 4");
                });
        range.subscribe(integer -> log.info("integer : {}", integer),
                error -> log.error("error", error));
    }

    @Test
    void subscribe4() {
        Flux<Integer> range = Flux.range(1, 4)
                .map(integer -> {
                    if (integer <= 3) return integer;
                    throw new RuntimeException("Got to 4");
                });
        range.subscribe(integer -> log.info("integer: {}", integer),
                error -> log.error("error", error),
                () -> log.info("Completed runnable start runing"));

        Flux<Integer> range1 = Flux.range(1, 4);
        range1.subscribe(integer -> log.info("integer: {}", integer),
                error -> log.error("error", error),
                () -> log.info("Completed runnable start runing"));
    }

    @Test
    void subscribe5() {
        Flux<Integer> range = Flux.range(1, 4);
        range.subscribe(integer -> log.info("integer: {}", integer),
                error -> log.error("error", error),
                () -> log.info("Completed runnable start runing"),
                subscription -> {
                   log.info("subscription: {}", subscription);
                   subscription.request(10);
                });
        range.subscribeWith(new Subscriber<Integer>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                log.info("subscription: {}", subscription);
                subscription.request(10);
            }

            @Override
            public void onNext(Integer integer) {
                log.info("integer: {}", integer);
            }

            @Override
            public void onError(Throwable t) {
                log.error("error", t);
            }

            @Override
            public void onComplete() {
                log.info("Completed runnable start runing");
            }
        });
    }

    @Test
    void generate_create_push() {
        //只能单线程，同步生成: 需要返回下一个初始值
        Flux<String> generate1 = Flux.generate(
                AtomicLong::new,
                (state, synchronousSink) -> {
                    long i = state.getAndIncrement();
                    log.info("generating: {}", i);
                    synchronousSink.next("3 x " + i + " = " + 3 * i);
                    if (i == 3) {
                        synchronousSink.complete();
                    }
                    return state;
                });
        generate1
                .subscribeOn(Schedulers.parallel(), false)
                .publishOn(Schedulers.immediate())
                .subscribe(s -> log.info("generate1 : {}", s));

        //可以异步，多线程发射
        Flux<String> stringFlux = Flux.create(fluxSink -> {
            log.info("creating");
            fluxSink.next("1");
            fluxSink.next("2");
            fluxSink.next("3");
            fluxSink.complete();
        }, FluxSink.OverflowStrategy.LATEST);

        stringFlux
                .subscribeOn(Schedulers.parallel(), false)
                .publishOn(Schedulers.immediate())
                .subscribe(s -> log.info("received : {}",s ));

        //push单线程，异步
        Flux<String> push = Flux.push(fluxSink -> {
            log.info("pushing");
            fluxSink.next("1");
            fluxSink.next("2");
            fluxSink.next("3");
            fluxSink.complete();
        }, FluxSink.OverflowStrategy.LATEST);
        push.subscribeOn(Schedulers.parallel(), false)
                .publishOn(Schedulers.immediate())
                .subscribe(s -> log.info("received push: {}",s ));
    }
}
