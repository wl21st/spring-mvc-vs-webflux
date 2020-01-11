package com.filichkin.blog.reactive;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
@Slf4j
public class Application {

    public static void main(String[] args) {

        // Application app = new Application();

        SpringApplication.run(Application.class, args);

    }

    public static void reactorCoreShowCase() {

        // Hot Stream
        List<Integer> elements = new ArrayList<>();

        ConnectableFlux<Object> publish = Flux.create(fluxSink -> {
            while(true) {
                fluxSink.next(System.currentTimeMillis());
            }
        }).publish();

        publish.subscribe(System.out::println);
        publish.subscribe(System.out::println);

        // This is blocking, should be added to another thread
        publish.connect();

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

