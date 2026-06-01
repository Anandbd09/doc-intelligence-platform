package com.docintel.embedding_service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DocUploadedEvent {
    private String docId;
    private String s3Key;
    private String userId;
}
