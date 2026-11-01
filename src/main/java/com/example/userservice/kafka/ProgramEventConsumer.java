package com.example.userservice.kafka;


import com.example.userservice.model.AuditLog;
import com.example.userservice.repository.AuditLogRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ProgramEventConsumer {
    private final AuditLogRepository auditLogRepository;
    private final ObjectMapper objectMapper;
    private static final Logger log = LoggerFactory.getLogger(ProgramEventConsumer.class);

    public ProgramEventConsumer(AuditLogRepository auditLogRepository, ObjectMapper objectMapper) {
        this.auditLogRepository = auditLogRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "program-events", groupId = "user-service-group")
    public void consume(String message) {
        log.info("Kafka received: " + message);
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
            log.info("Audit has save");

        } catch (Exception e) {
            log.error("Error audit save: " + e.getMessage());
        }
    }
}
