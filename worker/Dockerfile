# syntax=docker/dockerfile:1
# Build context: repository root (multi-module Maven project).

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY common/pom.xml common/
COPY controller/pom.xml controller/
COPY worker/pom.xml worker/
COPY common/src common/src
COPY worker/src worker/src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -pl worker -am package -DskipTests

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S app && adduser -S app -G app \
    && mkdir -p /data/chunks && chown -R app:app /data
WORKDIR /app
COPY --from=build /build/worker/target/worker.jar app.jar
USER app
# Chunk data lives on a per-worker named volume mounted here (see docker-compose.yml).
VOLUME ["/data"]
EXPOSE 8081
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError" \
    STORAGE_DIR=/data/chunks
HEALTHCHECK --interval=5s --timeout=3s --start-period=30s --retries=10 \
    CMD wget -qO- http://localhost:8081/actuator/health >/dev/null || exit 1
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

