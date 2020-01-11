package com.filichkin.blog.reactive;

import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@SpringBootApplication
@Slf4j
public class Application {

    public static void main(String[] args) {

        reactorCoreShowCase();

        SpringApplication.run(Application.class, args);

    }

    static class PriceTickerDataFeeder extends Thread {

        private final Listener listener;

        public PriceTickerDataFeeder(Listener listener) {
            this.listener = listener;    
        }

        // Implement the backpressure from the data source
        // Something like Kafka Consumer's Polling Thread
        public void run() {
            int count = 1;
            while (true){
                String e = "#" + (count++) +" @: " + System.currentTimeMillis();
                log.info("1.GENERATE: " + e);
                listener.onData(e);

                try {
                    sleep(100);
                } catch (InterruptedException ex) {
                    log.error("Thread interrupted:", ex);
                    break;
                }
            }
        }


    }

    interface Listener {
        void onData(String data);
    }

    public static PriceTickerDataFeeder getPriceTickerDataFeeder() {
        return null;
    }

    public static void reactorCoreShowCase() {

        Scheduler s = Schedulers.newParallel("parallel-scheduler", 4);

        final Flux<String> flux1 = Flux
                .range(1, 2)
                .map(i -> 10 + i)
                .publishOn(s)
                .map(i -> "value " + i);

        new Thread(() -> flux1.subscribe(e -> System.out.println("publisheOn() demo: " + Thread.currentThread().getName() + ":" + e))).start();

        // Parallel stream
        Flux.range(1, 10)
                .parallel(2)
                .subscribe(i -> System.out.println(Thread.currentThread().getName() + " -> " + i));

        Flux.range(1, 10)
                .parallel(2)
                .runOn(Schedulers.parallel())
                .subscribe(i -> System.out.println(Thread.currentThread().getName() + " -> " + i));

        // Hot Stream
        List<Integer> elements = new ArrayList<>();

        Flux<String> publisherFull = Flux.<String>create(new Consumer<FluxSink<String>>() {
            @Override
            public void accept(FluxSink<String> fluxSink) {

            }
        });

        ConnectableFlux<String> publisher = Flux.<String>create(fluxSink -> {

            Listener dataListener = new Listener() {

                int count = 0;
                @Override
                public void onData(String data) {
                    fluxSink.next(data);

                    count++;

                    if (count> 7) {
                        fluxSink.complete();
                    }
                }
            };

            PriceTickerDataFeeder feeder = new PriceTickerDataFeeder(dataListener);

            feeder.start();
        })
                .sample(Duration.ofMillis(100))
                .log()
                .publishOn(Schedulers.parallel())
                .publish();

//        publisher.subscribeOn(Schedulers.parallel());
        // publisher.subscribeOn(Schedulers.newSingle("thread-e1"));
        publisher.subscribeOn(Schedulers.newParallel("pool", 5)).subscribe(o -> log.info(String.format("2.SUBSCRIBE: %s", o)));
//        publisher.subscribe(o -> log.info(String.format("#2: %s", o)));

        // This is blocking, should be added to another thread
        publisher.connect();

        Mono.just(1)
                .log()
                .subscribe(elements::add);

        List<String> stringList = new ArrayList<>();

        Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .log()
                .map(i -> i * 2)
                .zipWith(Flux.range(0, Integer.MAX_VALUE),
                        (one, two) -> String.format("First Flux: %d, Second Flux: %d", one, two)).subscribe(new Subscriber<String>() {
            private Subscription subscription;
            int onNextAmount;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(2);
            }

            @Override
            public void onNext(String s) {
                log.info(String.format("Subscribe: %s", s));
                stringList.add(s);
                onNextAmount++;
                if (onNextAmount % 2 == 0) {
                    // Request the next elements
                    subscription.request(2);

                    // Cancel subscription using subscription.cancel();
                }
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        });


        Flux.just(1, 2, 3, 4)
                .log()
                .subscribe(elements::add);

        System.out.println(getList());

        Flux.fromIterable(getList()).delayElements(Duration.ofMillis(1))
                .map(d -> d * 2)
                .take(10)
                .subscribe(System.out::println);

        Flux<Integer> flux = Flux.fromIterable(getList());

        flux.delayElements(Duration.ofMillis(100))
                .doOnNext(i -> System.out.println(i))
                .map(d -> d * 2)
                .take(1)
                .subscribe(System.out::println);


        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static List<Integer> getList() {
        return List.of(1, 2, 3, 4, 5);
    }

}

