package com.example.userservice.kafka;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ProgramEventConsumer {
    @KafkaListener(topics = "program-events", groupId = "user-service-group")
    public void consume(String message) {
        System.out.println("Kafka received: " + message);
    }
}
