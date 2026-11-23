# ============================================================================
# Raw event emitter: streams DEBS CSV files into Kafka.
#
# Build:
#   docker build -t event-emitter ./event-emitter
#
# Run (mount the repo's data/ directory; the container streams the CSV into
# Kafka — all emitter settings come from environment variables, see
# EventEmitter.Config.fromEnv()):
#   docker run --rm \
#     -v "$(pwd)/data:/data:ro" \
#     -e KAFKA_SERVER=host.docker.internal:9092 \
#     -e SPEED_MULTIPLIER=100 \
#     event-emitter
#
# The default keeps historical event time in the payload only. If overriding
# RECORD_TIMESTAMP=event, configure the broker to retain historical records.
#
# The repo carries no Maven wrapper (no mvnw, empty .mvn/), so the build stage
# uses the official Maven image instead of ./mvnw. Both stages are multi-arch.
# ============================================================================

# ---------- build: compile the jar and collect runtime dependencies ----------
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /build

# Resolve dependencies first so code-only rebuilds skip the download.
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -B -DskipTests dependency:go-offline

COPY src ./src
# package: build the app jar.
# copy-dependencies: pull every transitive runtime jar (kafka-clients incl. the
# lz4/snappy/zstd codecs it needs at runtime, jackson-databind, opencsv, slf4j)
# into target/dependency so the JRE-only runtime stage can use a plain -cp.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -B -DskipTests package \
        dependency:copy-dependencies -DincludeScope=runtime \
        -DoutputDirectory=target/dependency

# ---------- runtime: JRE only, non-root, config via environment ----------
FROM eclipse-temurin:21-jre-jammy AS runtime

RUN useradd --system --create-home --shell /usr/sbin/nologin emitter
WORKDIR /app

COPY --from=build /build/target/eventemitter-*.jar app.jar
COPY --from=build /build/target/dependency/ dependency/

# Honour container memory limits (override with -e JAVA_TOOL_OPTIONS="...").
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0"

# CSV_PATH assumes the repo's data/ directory is mounted at /data.
ENV CSV_PATH=/data/src/debs2022-gc-trading-day-12-11-21.csv \
    TOPIC_NAME=price-events \
    KAFKA_SERVER=kafka:9092 \
    SPEED_MULTIPLIER=1.0 \
    MAX_SLEEP_MS=0 \
    RECORD_TIMESTAMP=append \
    EVENT_TIME_ZONE=Europe/Berlin \
    PRICE_FILTER=none \
    KAFKA_ACKS=all \
    KAFKA_IDEMPOTENCE=true

# The Java launcher expands the classpath wildcard in exec form.
USER emitter
ENTRYPOINT ["java", "-cp", "/app/app.jar:/app/dependency/*", "org.ultrastonk.eventemitter.Main"]
# --emit selects Kafka replay. Pass --stats to inspect CSV_PATH without Kafka.
CMD ["--emit"]
