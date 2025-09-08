# We need an image with a full JDK to compile and run the app
FROM openjdk:17
WORKDIR /app
COPY target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]