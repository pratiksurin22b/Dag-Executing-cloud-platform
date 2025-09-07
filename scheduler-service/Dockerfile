# Use the correct and available official tag for a minimal Java 17 environment
FROM openjdk:17-jdk-slim

# The rest of the file remains exactly the same
WORKDIR /app
COPY target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]

