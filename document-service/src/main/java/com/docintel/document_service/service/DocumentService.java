package com.docintel.document_service.service;

import com.docintel.document_service.dto.response.DocumentResponse;
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
import java.util.UUID;

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

    public DocumentResponse uploadDocument(MultipartFile file, String userId){

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

        DocumentEntity saved = repository.save(document);

        // Step 4 — publish Kafka event
        String message = String.format(  "{\"docId\":\"%s\",\"s3Key\":\"%s\",\"userId\":\"%s\"}", saved.getId(), saved.getS3Key(),saved.getUserId());
        kafkaTemplate.send(docUploadedTopic, saved.getId(), message);

        // Step 5 — return DocumentResponse

        DocumentResponse response = new DocumentResponse();
        response.setId(saved.getId());
        response.setName(saved.getName());
        response.setContentType(saved.getContentType());
        response.setSize((long) Math.toIntExact(saved.getSize()));
        response.setStatus(saved.getStatus());
        response.setCreatedOn(saved.getCreatedOn());
        response.setUpdatedOn(saved.getUpdatedOn());
        return response;
    }
}
