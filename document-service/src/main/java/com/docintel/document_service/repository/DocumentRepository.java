package com.docintel.document_service.repository;

import com.docintel.document_service.model.DocumentEntity;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface DocumentRepository extends MongoRepository<DocumentEntity,String> {
    List<DocumentEntity> findByUserId(String userId);
}
