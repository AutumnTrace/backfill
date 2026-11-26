FROM eclipse-temurin:21.0.13.1_2-jdk-noble AS build
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
RUN ./mvnw -q -B dependency:go-offline
COPY src src
RUN ./mvnw -q -B -DskipTests package

FROM eclipse-temurin:21.0.13.1_2-jre-noble
WORKDIR /app
COPY --from=build /src/target/backfill.jar backfill.jar
COPY scripts scripts
COPY data/scans-demo.ndjson data/scans-demo.ndjson
ENV BACKFILL_JAR=/app/backfill.jar
ENTRYPOINT ["java", "-jar", "/app/backfill.jar"]
