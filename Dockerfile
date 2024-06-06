# docker build -t montplex/redis-vlog:1.0.0 .
# docker tag montplex/redis-vlog:1.0.0 registry.ap-southeast-1.aliyuncs.com/montplex/redis-vlog:1.0.0
FROM eclipse-temurin:21-jdk-jammy

WORKDIR /montplex
VOLUME /var/lib/redis-vlog

#COPY agent/build/libs/redis-vlog-agent-1.0.0.jar /montplex/redis-vlog-agent-1.0.0.jar
COPY build/libs/redis-vlog-1.0.0.jar /montplex/redis-vlog-1.0.0.jar

### -javaagent:redis-vlog-agent-1.0.0.jar
CMD java -Xmx2g -Xms2g -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=128m -jar redis-vlog-1.0.0.jar