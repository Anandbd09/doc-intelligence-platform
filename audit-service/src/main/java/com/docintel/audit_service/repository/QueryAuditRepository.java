package com.docintel.audit_service.repository;

import com.docintel.audit_service.model.QueryAuditEntity;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface QueryAuditRepository extends MongoRepository<QueryAuditEntity, String> {
}
