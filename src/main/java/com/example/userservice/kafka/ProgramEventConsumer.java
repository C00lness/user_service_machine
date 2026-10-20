package com.example.userservice.kafka;


import com.example.userservice.model.AuditLog;
import com.example.userservice.repository.AuditLogRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ProgramEventConsumer {
    private final AuditLogRepository auditLogRepository;
    private final ObjectMapper objectMapper;

    public ProgramEventConsumer(AuditLogRepository auditLogRepository, ObjectMapper objectMapper) {
        this.auditLogRepository = auditLogRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "program-events", groupId = "user-service-group")
    public void consume(String message) {
        System.out.println("Kafka received: " + message);
        try {
            // Парсим JSON
            JsonNode jsonNode = objectMapper.readTree(message);

            AuditLog auditLog = new AuditLog();
            auditLog.setEventType(jsonNode.get("event").asText());
            auditLog.setProgramId(jsonNode.get("programId").asLong());
            auditLog.setProgramName(jsonNode.get("programName").asText());
            auditLog.setCreatedBy(jsonNode.get("createdBy").asText());

            // Сохраняем в БД
            auditLogRepository.save(auditLog);
            System.out.println("Audit has save");

        } catch (Exception e) {
            System.err.println("Error audit save: " + e.getMessage());
        }
    }
}
