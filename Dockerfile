FROM openjdk:8-jdk-alpine
ADD target/notifications.jar notifications.jar
ENTRYPOINT ["java", "-jar", "notifications.jar"]
EXPOSE 8082