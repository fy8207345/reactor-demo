package org.fy.reactor.tutorial;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

public class ReactorTests {
    @Test
    void test1() {
        Flux.just("hello")
        .subscribe(System.out::println);
    }

    @Test
    void test2() {
        List<String> words = Arrays.asList(
                "the",
                "quick",
                "brown",
                "fox",
                "jumps",
                "over",
                "the",
                "lazy",
                "dog"
        );
        Flux.fromIterable(words)
                .flatMap(word -> Flux.fromArray(word.split("")))
                .distinct()
                .sort()
                .zipWith(Flux.range(1, 100), (word, line) -> line + ". " + word)
                .subscribe(System.out::println);
    }

    @Test
    void test3() throws InterruptedException {
        Flux fastClock = Flux.interval(Duration.ofSeconds(1))
                .map(tick -> "fast " + tick);

        Flux slowClock = Flux.interval(Duration.ofSeconds(2))
                .map(tick -> "slow " + tick);

        Flux clock = Flux.merge(
                fastClock.filter(tick -> isFastTime()),
                slowClock.filter(tick -> isSlowTime())
        );
//        clock.publishOn(Schedulers.immediate()).subscribeOn(Schedulers.immediate()).subscribe(System.out::println);

        Flux feed = Flux.interval(Duration.ofSeconds(1))
                .map(tick -> LocalTime.now());
        clock.withLatestFrom(feed, (tick, time)  -> tick + " " + time)
        .subscribe(System.out::println);
        Thread.sleep(10000);
    }

    @Test
    void test5() {

    }

    private boolean isSlowTime() {
        return true;
    }

    private boolean isFastTime() {
        return true;
    }
}
