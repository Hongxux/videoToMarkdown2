FROM maven:3.9.9-eclipse-temurin-17 AS builder

WORKDIR /workspace

COPY services/java-orchestrator /workspace/services/java-orchestrator
COPY contracts/proto /workspace/contracts/proto

RUN mvn -f /workspace/services/java-orchestrator/pom.xml clean package -DskipTests

FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=builder /workspace/services/java-orchestrator/target/fusion-orchestrator-0.0.1-SNAPSHOT.jar /app/fusion-orchestrator.jar

EXPOSE 8080

CMD ["java", "-jar", "/app/fusion-orchestrator.jar"]
