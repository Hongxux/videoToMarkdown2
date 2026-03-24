# syntax=docker/dockerfile:1.7
FROM maven:3.9.9-eclipse-temurin-17 AS builder

WORKDIR /workspace

COPY services/java-orchestrator/pom.xml /workspace/services/java-orchestrator/pom.xml
COPY contracts/proto /workspace/contracts/proto

RUN --mount=type=cache,target=/root/.m2 \
    /bin/sh -c 'attempt=1; while [ "$attempt" -le 3 ]; do mvn -f /workspace/services/java-orchestrator/pom.xml -DskipTests -Dhttps.protocols=TLSv1.2 dependency:go-offline -q && exit 0; echo "[java-orchestrator] dependency:go-offline failed, retry=$attempt"; attempt=$((attempt+1)); sleep 5; done; exit 1'

COPY services/java-orchestrator/src /workspace/services/java-orchestrator/src

RUN --mount=type=cache,target=/root/.m2 \
    mvn -f /workspace/services/java-orchestrator/pom.xml clean package -Dmaven.test.skip=true -Dhttps.protocols=TLSv1.2 -q \
    && cp "$(find /workspace/services/java-orchestrator/target -maxdepth 1 -type f -name 'fusion-orchestrator-*.jar' ! -name '*.original' | head -n 1)" /workspace/fusion-orchestrator.jar

FROM eclipse-temurin:17-jre

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/fusion-orchestrator.jar /app/fusion-orchestrator.jar

EXPOSE 8080

CMD ["java", "-jar", "/app/fusion-orchestrator.jar"]
