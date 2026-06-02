package com.docintel.document_service.controller;

import com.docintel.document_service.dto.response.DocumentResponse;
import com.docintel.document_service.service.DocumentService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("/api/documents")
@RequiredArgsConstructor
public class DocumentController {
    private final DocumentService documentService;

    @PostMapping
    public ResponseEntity<DocumentResponse> uploadDocument(
            @RequestParam MultipartFile file,
            @RequestParam String userId){
        return ResponseEntity.ok(documentService.uploadDocument(file, userId));
    }

    @GetMapping("/{userId}")
    public ResponseEntity<List<DocumentResponse>> getDocumentByUserId(@PathVariable String userId){
        return ResponseEntity.ok(documentService.getDocumentByUserId(userId));
    }

    @GetMapping("/{userId}/{documentId}/status")
    public ResponseEntity<DocumentResponse> getDocumentStatus(@PathVariable String userId, @PathVariable String documentId){
        return ResponseEntity.ok(documentService.getDocumentStatus(documentId));

    }

    @DeleteMapping("/{userId}/{docId}")
    public ResponseEntity<Void> deleteDocument(
            @PathVariable String userId,
            @PathVariable String docId
    ) {
        documentService.deleteDocument(userId, docId);
        return ResponseEntity.noContent().build();
    }
}
