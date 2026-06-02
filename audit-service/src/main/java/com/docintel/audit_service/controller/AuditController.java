package com.docintel.audit_service.controller;

import com.docintel.audit_service.model.QueryAuditEntity;
import com.docintel.audit_service.repository.QueryAuditRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/audit")
@RequiredArgsConstructor
public class AuditController {

    private final QueryAuditRepository queryAuditRepository;

    @GetMapping("/{userId}")
    public ResponseEntity<List<QueryAuditEntity>> getAuditLogs(
            @PathVariable String userId) {
        return ResponseEntity.ok(
                queryAuditRepository.findByUserIdOrderByTimestampDesc(userId)
        );
    }
}