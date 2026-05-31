package com.docintel.document_service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;


import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "documents")
public class DocumentEntity {

    @Id
   private String id;
    private String userId;
   private String name;
   private String s3Key;
   private Long size;
   private LocalDateTime createdOn;
   private LocalDateTime updatedOn;
   private String contentType;
   private DocumentStatus status;
}
