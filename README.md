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

## Summary

### Service Using WebFlux

1. The performance of service using WebFlux is NOT good.
    + It neither shows a better performance nor has a lower failure rate.  Actually, there is 1% of 500 found during testing of WebFlux.  
So in general, this is NOT a good direction to go.
1. Stable version and combination
    + So far, the best result achieved is using the apache client completed future + Spring Boot 2.1.18.RELEASE.  It uses Tomcat 9.0.39, KO: 351
    + The original 2.1.2.RELEASE is quite poor performance.  2.3.11.RELEASE, 2.5.0, 2.4.6 don't give better result either and have multiple 500 errors.
    + 2.2.13.RELEASE gives good result.  Tomcat 9.0.41, KO: 213
    
