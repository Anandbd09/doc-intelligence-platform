package com.docintel.document_service.service;

import com.docintel.document_service.dto.response.DocumentResponse;
import com.docintel.document_service.exception.ResourceNotFoundException;
import com.docintel.document_service.model.DocumentEntity;
import com.docintel.document_service.model.DocumentStatus;
import com.docintel.document_service.repository.DocumentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class DocumentService {

    private final DocumentRepository repository;
    private final S3Client s3Client;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    @Value("${app.kafka.topic.doc-uploaded}")
    private String docUploadedTopic;

    @Value("${app.kafka.topic.doc-deleted}")
    private String docDeletedTopic;

    public DocumentResponse uploadDocument(MultipartFile file, String userId){

        // Generate content hash for duplicate detection
        String contentHash;
        try {
            contentHash = generateHash(file.getBytes());
        } catch (IOException e) {
            throw new RuntimeException("Failed to read file content");
        }

// Check for duplicate content
        Optional<DocumentEntity> existing = repository.findByUserIdAndContentHash(userId, contentHash);
        if (existing.isPresent()) {
            System.out.println("Duplicate content detected - returning existing document");
            return mapToResponse(existing.get());
        }

        // Validate file type
        String contentType = file.getContentType();
        if (contentType == null ||
                (!contentType.equals("application/pdf") &&
                        !contentType.equals("image/png") &&
                        !contentType.equals("image/jpeg"))) {
            throw new IllegalArgumentException(
                    "Unsupported file type: " + contentType +
                            ". Supported types: PDF, PNG, JPG"
            );
        }

        // Step 1 — generate unique S3 key
        String s3Key = userId + "/" + UUID.randomUUID()+ "-" + file.getOriginalFilename();

        // Step 2 - upload to s3

        try{
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .contentType(file.getContentType())
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(file.getBytes()));
        }catch(IOException e){
            throw new RuntimeException("Failed to upload file to s3", e);
        }

        // Step 3 — build and save DocumentEntity to MongoDB

        DocumentEntity document = new DocumentEntity();
        document.setUserId(userId );
        document.setName(file.getOriginalFilename());
        document.setS3Key(s3Key);
        document.setSize(file.getSize());
        document.setContentType(file.getContentType());
        document.setStatus(DocumentStatus.UPLOADING);
        document.setCreatedOn(LocalDateTime.now());
        document.setUpdatedOn(LocalDateTime.now());

        document.setContentHash(contentHash);
        DocumentEntity saved = repository.save(document);

        // Step 4 — publish Kafka event
        String message = String.format(  "{\"docId\":\"%s\",\"s3Key\":\"%s\",\"userId\":\"%s\"}", saved.getId(), saved.getS3Key(),saved.getUserId());
        kafkaTemplate.send(docUploadedTopic, saved.getId(), message);

        // Step 5 — return DocumentResponse

        return mapToResponse(saved);

    }

    private String generateHash(byte[] content) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(content);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate hash");
        }
    }



    public List<DocumentResponse> getDocumentByUserId(String userId){
        List<DocumentEntity> documents = repository.findByUserId(userId);
        return documents.stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }
    private DocumentResponse mapToResponse(DocumentEntity document){
        DocumentResponse response = new DocumentResponse();
        response.setId(document.getId());
        response.setName(document.getName());
        response.setContentType(document.getContentType());
        response.setSize(document.getSize());
        response.setStatus(document.getStatus());
        response.setCreatedOn(document.getCreatedOn());
        response.setUpdatedOn(document.getUpdatedOn());
        response.setTags(document.getTags());
        response.setProgress(document.getProgress());
        return response;
    }

    public DocumentResponse getDocumentStatus(String documentId){
        DocumentEntity document = repository.findById(documentId)
                .orElseThrow(() -> new ResourceNotFoundException("Document not found with id: " + documentId));
        return mapToResponse(document);
    }


    public void deleteDocument(String userId, String docId) {
        // Step 1 - find document
        DocumentEntity document = repository.findById(docId)
                .orElseThrow(() -> new ResourceNotFoundException("Document not found: " + docId));

        // Step 2 - delete from S3
        try {
            software.amazon.awssdk.services.s3.model.DeleteObjectRequest deleteRequest =
                    software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(document.getS3Key())
                            .build();
            s3Client.deleteObject(deleteRequest);
            System.out.println("Deleted from S3: " + document.getS3Key());
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete from S3: " + e.getMessage());
        }

        // Step 3 - delete from MongoDB
        repository.deleteById(docId);
        System.out.println("Deleted from MongoDB: " + docId);

        // Step 4 - publish doc-deleted event
        String message = String.format(
                "{\"docId\":\"%s\",\"userId\":\"%s\"}", docId, userId);
        kafkaTemplate.send(docDeletedTopic, docId, message);
        System.out.println("Published doc-deleted event for: " + docId);
    }

    public DocumentResponse reprocessDocument(String userId, String docId) {
        DocumentEntity document = repository.findById(docId)
                .orElseThrow(() -> new ResourceNotFoundException("Document not found: " + docId));

        // Reset status to UPLOADING
        document.setStatus(DocumentStatus.UPLOADING);
        document.setUpdatedOn(LocalDateTime.now());
        repository.save(document);

        // Re-publish doc-uploaded event
        String message = String.format(
                "{\"docId\":\"%s\",\"s3Key\":\"%s\",\"userId\":\"%s\"}",
                document.getId(), document.getS3Key(), document.getUserId());
        kafkaTemplate.send(docUploadedTopic, document.getId(), message);

        System.out.println("Re-processing document: " + docId);
        return mapToResponse(document);
    }
}
