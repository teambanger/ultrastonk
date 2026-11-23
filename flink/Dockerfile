FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY flink/pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp dependency:go-offline
COPY flink/src src
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp verify

FROM flink:1.20.3-scala_2.12-java17
COPY --from=build /build/target/price-pipeline.jar /opt/flink/usrlib/price-pipeline.jar
COPY --chmod=755 flink/entrypoint.sh /opt/flink/pipeline-entrypoint.sh
COPY flink/log4j-console.properties /opt/flink/conf/log4j-console.properties
USER root
RUN mkdir -p /opt/flink/state && chown flink:flink /opt/flink/state
USER flink
ENTRYPOINT ["/opt/flink/pipeline-entrypoint.sh"]
