package com.docintel.document_service.dto.response;

import com.docintel.document_service.model.DocumentStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;

import java.time.LocalDateTime;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DocumentResponse {

    private String id;
    private String name;
    private String contentType;
    private Long size;
    private DocumentStatus status;
    private LocalDateTime createdOn;
    private LocalDateTime updatedOn;
    private List<String> tags;
    private String progress;
}
