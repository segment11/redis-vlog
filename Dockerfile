# docker build -t montplex/redis-vlog:1.0.0 .
# need `gradle jar` first
FROM eclipse-temurin:21-jdk-jammy

WORKDIR /montplex
VOLUME /var/lib/redis-vlog
VOLUME /etc/redis-vlog.properties

COPY build/libs/dyn /montplex/dyn
COPY build/libs/log4j2.xml /montplex/log4j2.xml
COPY build/libs/lib /montplex/lib
COPY build/libs/redis-vlog-1.0.0.jar /montplex/redis-vlog-1.0.0.jar

CMD java -Xmx2g -Xms2g -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=128m -jar redis-vlog-1.0.0.jar