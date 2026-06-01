package com.docintel.audit_service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor@NoArgsConstructor
@Document(collection = "query-audits")
public class QueryAuditEntity {

    @Id
    private String id;
    private String userId;
    private String question;
    private String answer;
    private LocalDateTime timestamp;
    private String documentsQueried;
}
