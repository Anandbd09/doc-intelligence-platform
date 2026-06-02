package com.docintel.audit_service.repository;

import com.docintel.audit_service.model.QueryAuditEntity;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface QueryAuditRepository extends MongoRepository<QueryAuditEntity, String> {
    List<QueryAuditEntity> findByUserIdOrderByTimestampDesc(String userId);
}
