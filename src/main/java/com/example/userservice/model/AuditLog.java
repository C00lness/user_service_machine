package com.example.userservice.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Entity
@Table(name = "audit_logs")
@Data
@NoArgsConstructor
public class AuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String eventType;

    private Long programId;

    private String programName;

    private String createdBy;

    private LocalDateTime receivedAt;

    @PrePersist
    protected void onCreate() {
        this.receivedAt = LocalDateTime.now();
    }
}
