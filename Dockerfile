# docker build -t montplex/redis-vlog:1.0.0 .
FROM eclipse-temurin:21-jdk-jammy

WORKDIR /montplex
VOLUME /var/lib/redis-vlog
VOLUME /etc/redis-vlog.properties

COPY build/libs/redis-vlog-1.0.0.jar /montplex/redis-vlog-1.0.0.jar
COPY build/libs/dyn /montplex/dyn

CMD java -Xmx2g -Xms2g -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=128m -jar redis-vlog-1.0.0.jar