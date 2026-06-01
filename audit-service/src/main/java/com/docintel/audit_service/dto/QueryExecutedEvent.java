package com.docintel.audit_service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor@NoArgsConstructor
public class QueryExecutedEvent {
    private String userId;
    private String question;
    private String answer;
}
