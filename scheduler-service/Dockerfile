# Runtime image for Java 17 using Eclipse Temurin JRE (stable and small)
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app
COPY target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
