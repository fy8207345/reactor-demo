package org.fy.reactor;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;

@Slf4j
public class SampleSubscriber<T> extends BaseSubscriber<T> {

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
        log.info("subscribed : {}", subscription);
        subscription.request(1);
        super.hookOnSubscribe(subscription);
    }

    @Override
    protected void hookOnNext(T value) {
        log.info("onNext: {}", value);
        super.hookOnNext(value);
    }

    @Override
    protected void hookFinally(SignalType type) {
        log.info("finnaly:{}", type);
        super.hookFinally(type);
    }

    @Override
    protected void hookOnComplete() {
        log.info("complete");
        super.hookOnComplete();
    }

    @Override
    protected void hookOnError(Throwable throwable) {
        log.error("error", throwable);
        super.hookOnError(throwable);
    }

    @Override
    protected void hookOnCancel() {
        log.info("canceld subscription");
        super.hookOnCancel();
    }

    @Test
    void testSubscribe() {
        SampleSubscriber<Integer> sampleSubscriber = new SampleSubscriber<>();
        Flux<Integer> range = Flux.range(1, 3);
        range.subscribe(sampleSubscriber);
    }
}
