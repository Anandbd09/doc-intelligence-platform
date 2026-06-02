package com.docintel.embedding_service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DocEmbeddedEvent {
    private String docId;
    private String userId;
    private String extractedText;
}