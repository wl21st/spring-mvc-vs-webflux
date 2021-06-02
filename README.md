# spring-mvc-vs-webflux

https://medium.com/@filia.aleks/microservice-performance-battle-spring-mvc-vs-webflux-80d39fd81bf0

Compare Performance for Rest Service that proxies request to underline Rest service and return it back.

+ Servlet blocking call with Java 11 client (endpoint /sync)
+ Servlet with CompletableFuture call with Java 11 client (endpoint /completable-future)
+ WebFlux call with Java 11 client (endpoint /webflux-java-http-client)
+ WebFlux call with WebClient (endpoint /webflux-webclient)
+ WebFlux call with Apache Http Client (endpoint /webflux-apache-client)

## Build And Test

### Build

1. For WebFlux(Netty) use "web-flux" maven profile, `mvn clean install -P web-flux`.  It will use the netty to run the service.
1. For Servlet(Tomcat) use "servlet" maven profile, `mvn clean install -P servlet`.  It will use tomcat to run the service.

### Test

Type `mvn gatlin:test` to run the performance test

### Run Test Once

Type `mvn clean install -P servlet && java -jar target/spring-mvc-vs-webflux-0.0.1-SNAPSHOT.jar &  sleep 7 && mvn gatling:test && kill %1`     


## Summary

### Service Using WebFlux

1. The performance of service using WebFlux is NOT good.
    + It neither shows a better performance nor has a lower failure rate.  Actually, there is 1% of 500 found during testing of WebFlux.  
So in general, this is NOT a good direction to go.
1. Stable version and combination is Tomcat + Future
    + So far, the best result achieved is using the apache client completed future + Spring Boot 2.1.18.RELEASE.  It uses Tomcat 9.0.39, KO: 351    
   + 2.2.13.RELEASE gives good result.  Tomcat 9.0.41, KO: 213
   + 2.3.11.RELEASE gives good result.  Tomcat 9.0.46, KO: 116, avg: 160ms, 99% 665ms, min: 21ms, max=1,324ms
   + 2.4.6 gives good result.  Tomcat 9.0.46, KO: 137, avg: 190ms, 99%: 746ms, min: 43ms, max: 1,798ms
   + 2.5.0 didn't give good result.  Tomcat 9.0.46, KO: 908, avg: 201ms, 99%: 703ms, min: 3ms, max: 2,416ms
   + The original 2.1.2.RELEASE didn't give good performance as well as 2.5.0.
    
