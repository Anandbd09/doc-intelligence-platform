package com.docintel.document_service.controller;

import com.docintel.document_service.dto.response.DocumentResponse;
import com.docintel.document_service.service.DocumentService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

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
}
