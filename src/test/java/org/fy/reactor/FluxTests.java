package org.fy.reactor;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.*;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FluxTests {

    @Test
    void subscribe1() {
        int a = 1_000;
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

    @Test
    void push_pull() {
        AtomicLong atomicLong = new AtomicLong(0);
        Flux
                .create((Consumer<FluxSink<Long>>) fluxSink -> {
                    fluxSink.onRequest(value -> {
                        log.info("onRequest:{}", value);
                        for(long i=0; i<value;i++){
                            fluxSink.next(atomicLong.getAndIncrement());
                        }
                    });
                    fluxSink.onCancel(() -> {
                        log.info("canceld");
                    });
                    fluxSink.onDispose(() -> {
                        log.info("dispose");
                    });
                })
                .limitRequest(100)
                .limitRate(10, 5)
                .subscribe(new BaseSubscriber<Long>() {
            @Override
            protected void hookOnComplete() {
                log.info("hookOnComplete");
            }

            @Override
            protected void hookOnNext(Long value) {
                log.info("hookOnNext:{}", value);
                super.request(1);
            }

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                subscription.request(1);
            }
        });
    }

    @Test
    void block() {
        Integer integer = Flux.just(1, 3)
                .blockFirst();
        log.info("blockFirst:{}", integer);
        Integer integer1 = Flux.range(1, 3)
                .blockLast();
        log.info("blockLast:{}", integer1);
        Iterable<Integer> integers = Flux.range(1, 3)
                .toIterable();
        log.info("toIterable:{}", integers);
        Stream<Integer> integerStream = Flux.range(1, 3)
                .toStream();
        log.info("toStream:{}", integerStream.map(Object::toString).collect(Collectors.joining(",")));
    }

    @Test
    void buffer() {
        Flux.range(1,99)
                .buffer(5)
                .subscribe(new BaseSubscriber<List<Integer>>() {
                    @Override
                    protected void hookOnNext(List<Integer> value) {
                        log.info("hookOnNext:{}", value);
                        request(1);
                    }

                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        //取n个buffer！！
                        subscription.request(1);
                    }

                    @Override
                    protected void hookOnComplete() {
                        log.info("complete");
                    }
                });
    }

    @Test
    void handle() {

        Flux<Integer> range = Flux.range(1, 3);
            range
                .handle((integer, synchronousSink) -> {
                    synchronousSink.next( integer * 2);
                 })
                .subscribe(integer -> {
                    log.info("received : {}", integer);
                });
    }

    @Test
    void thread() throws InterruptedException {
        final Mono<String> mono = Mono.just("test");

        Thread t = new Thread(() -> {
            mono.map(s -> s + "thread")
                    .subscribe(s -> {
                        log.info(s + Thread.currentThread().getName());
                    });
        });
        t.start();
        t.join();
    }

    @Test
    void schedulers() throws InterruptedException {
        // 如果不指定，source默认在subscribe的线程
        Flux.create(fluxSink -> {
            log.info("Schedulers.immediate() source creating");
            fluxSink.next(1);
            fluxSink.next(2);
            fluxSink.complete();
        })
                .subscribeOn(Schedulers.immediate()) // 当前线程执行runnable，不另起线程
                .subscribe(integer -> {
                    log.info("Schedulers.immediate() recevied: {}", integer);
                });

        Flux.create(fluxSink -> {
            log.info("Schedulers.single() source creating");
            fluxSink.next(1);
            fluxSink.next(2);
            fluxSink.complete();
        })
                .subscribeOn(Schedulers.single())
                .subscribe(integer -> {
                    log.info("Schedulers.single() recevied: {}", integer);
                });

        Flux.create(fluxSink -> {
            log.info("Schedulers.elastic() source creating");
            fluxSink.next(1);
            fluxSink.next(2);
            fluxSink.complete();
        })
                .subscribeOn(Schedulers.elastic()) //废弃，无限制的线程数量
                .subscribe(integer -> {
                    log.info("Schedulers.elastic() recevied: {}", integer);
                });

        Flux.create(fluxSink -> {
            log.info("Schedulers.boundedElastic() source creating");
            fluxSink.next(1);
            fluxSink.next(2);
            fluxSink.complete();
        })
                .subscribeOn(Schedulers.boundedElastic()) //最多可以创建核心数量 * 10个线程
                .subscribe(integer -> {
                    log.info("Schedulers.boundedElastic() recevied: {}", integer);
                });

        Flux.create(fluxSink -> {
            log.info("Schedulers.parallel() source creating");
            fluxSink.next(1);
            fluxSink.next(2);
            fluxSink.complete();
        })
                .subscribeOn(Schedulers.parallel()) //最多可以创建核心数量个线程
                .subscribe(integer -> {
                    log.info("Schedulers.parallel() recevied: {}", integer);
                });

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Flux.create(fluxSink -> {
            log.info("Schedulers.fromExecutor() source creating");
            fluxSink.next(1);
            fluxSink.next(2);
            fluxSink.complete();
        })
                .subscribeOn(Schedulers.fromExecutor(executorService))
                .subscribe(integer -> {
                    log.info("Schedulers.fromExecutor() recevied: {}", integer);
                });

        Flux.create((Consumer<FluxSink<Integer>>)fluxSink -> {
            log.info("create");
            fluxSink.next(1);
            fluxSink.complete();
        })
                .publishOn(Schedulers.single()) //指定后续的操作所在的线程
                .map(integer -> {
                    log.info("map1 : {}", integer);
                    return integer * 2;
                })
                .publishOn(Schedulers.parallel()) //指定后续的操作所在的线程
                .map(integer -> {
                    log.info("map2 : {}", integer);
                    return integer.toString();
                })
                .publishOn(Schedulers.boundedElastic()) //指定后续的操作所在的线程
                .subscribeOn(Schedulers.fromExecutorService(executorService)) //指定source所在的线程
                .subscribeOn(Schedulers.parallel()) //第二次指定无效？？
                .subscribe(s -> {
                    log.info("recevied: {}", s);
                });

        Scheduler s = Schedulers.newParallel("parallel-scheduler", 4);
        Flux<String> map = Flux.range(1, 2)
                .map(integer -> integer + 10)
                .subscribeOn(s) //指定整个执行流所在的线程, publishOn可以改变下流的线程
                .map(integer -> "value " + integer);
        Thread thread = new Thread(() -> map.subscribe(s1 -> log.info("rec: {}", s1)));
        thread.start();
        thread.join();
    }

    @Test
    void error() {
        Flux<String> stringFlux = Flux.just(1, 2, 0)
                .map(integer -> "100 / " + integer + " = " + (100 / integer))
                .onErrorReturn("Divided by zero :(");
        stringFlux.subscribe(s -> {
            log.info("recv: {}", s);
        });

        Flux<String> stringFlux1 = Flux.just(1, 2, 0)
                .map(integer -> "100 / " + integer + " = " + (100 / integer))
                .onErrorResume(throwable -> Mono.just("Divided by zero!"));
        stringFlux1.subscribe(s -> log.info("recv: {}", s));
    }

    @Test
    void onErrorMap() {
        Flux<String> stringFlux2 = Flux.just(1, 2, 0)
                .map(integer -> "100 / " + integer + " = " + (100 / integer))
                .onErrorMap(throwable -> new BusinessException("divided by zero!", throwable));
        stringFlux2.subscribe(s -> log.info("recv: {}", s),
                throwable -> log.error("error : ", throwable));
    }

    @Test
    void doOnError() {
        Flux<String> stringFlux2 = Flux.just(1, 2, 0)
                .map(integer -> "100 / " + integer + " = " + (100 / integer))
                .doOnError(throwable -> log.error("error : ", throwable));
        stringFlux2.subscribe(s -> log.info("recv: {}", s));
    }
}
