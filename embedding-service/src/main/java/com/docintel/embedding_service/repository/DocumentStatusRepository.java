package com.docintel.embedding_service.repository;

import com.docintel.embedding_service.model.DocumentEntity;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface DocumentStatusRepository extends MongoRepository<DocumentEntity, String> {
}
