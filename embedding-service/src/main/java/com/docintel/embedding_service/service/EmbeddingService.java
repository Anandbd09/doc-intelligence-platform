package com.docintel.embedding_service.service;

import com.docintel.embedding_service.dto.DocUploadedEvent;
import com.docintel.embedding_service.model.DocumentEntity;
import com.docintel.embedding_service.repository.DocumentStatusRepository;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.chroma.ChromaEmbeddingStore;
import lombok.RequiredArgsConstructor;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import dev.langchain4j.model.output.Response;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class EmbeddingService {
    private final DocumentStatusRepository documentStatusRepository;
    private final S3Client s3Client;
    private final EmbeddingModel embeddingModel;
    private final TextractClient textractClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final long TEXTRACT_MAX_SIZE = 10 * 1024 * 1024; // 10MB
    private final org.springframework.data.mongodb.core.MongoTemplate mongoTemplate;

    @Value("${app.kafka.topic.doc-embedded}")
    private String docEmbeddedTopic;
    @Value("${chromadb.url}")
    private String chromaUrl;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    public byte[] downloadFromS3(String s3key){
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3key)
                .build();

        return s3Client.getObjectAsBytes(getObjectRequest).asByteArray();
    }

    public String extractTextFromPdf(byte[] pdfBytes){
        try (PDDocument document = Loader.loadPDF(pdfBytes)) {
            PDFTextStripper stripper = new PDFTextStripper();
            String text = stripper.getText(document);
            return text;
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract from pdf");
        }
    }

    public List<String> splitIntoChunks(String text, int chunkSize, int overlap){
        List<String> chunks = new ArrayList<>();  // before loop
        int start = 0;
        while (start < text.length()) {
            int end = Math.min(start + chunkSize, text.length());
            chunks.add(text.substring(start, end));  // inside loop
            start += chunkSize - overlap;             // inside loop
        }
        return chunks;  // after loop
    }

    public List<Float> generateEmbedding(String chunk){
        Response<Embedding> response = embeddingModel.embed(TextSegment.from(chunk));
        return response.content().vectorAsList();
    }


    public void storeEmbedding(String userId,String docId, String chunk, List<Float> vector){
        EmbeddingStore<TextSegment> store = ChromaEmbeddingStore.builder()
                .baseUrl(chromaUrl)
                .collectionName("embeddings_" + userId + "_" + docId)
                .build();

        Embedding embedding = Embedding.from(vector);
        TextSegment segment = TextSegment.from(chunk);
        store.add(embedding, segment);
    }

    public void updateDocumentStatus(String docId, String status){
        org.springframework.data.mongodb.core.query.Query query =
                new org.springframework.data.mongodb.core.query.Query(
                        org.springframework.data.mongodb.core.query.Criteria.where("_id").is(docId)
                );
        org.springframework.data.mongodb.core.query.Update update =
                new org.springframework.data.mongodb.core.query.Update().set("status", status);
        mongoTemplate.updateFirst(query, update, DocumentEntity.class);
    }

    public void processDocument(DocUploadedEvent event){
        System.out.println("Starting processing for doc: " + event.getDocId());

        // Ensure ChromaDB collection exists (get or create)
        try {
            org.springframework.web.client.RestTemplate restTemplate =
                    new org.springframework.web.client.RestTemplate();
            org.springframework.http.HttpHeaders headers =
                    new org.springframework.http.HttpHeaders();
            headers.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);
            String body = "{\"name\":\"embeddings_" + event.getUserId() + "_" +
                    event.getDocId() + "\",\"get_or_create\":true}";
            restTemplate.postForObject(
                    chromaUrl + "/api/v1/collections",
                    new org.springframework.http.HttpEntity<>(body, headers),
                    String.class
            );
        } catch (Exception ignored) {}


        byte[] pdfBytes = downloadFromS3(event.getS3Key());
        System.out.println("Downloaded PDF, size: " + pdfBytes.length);

        // Step 2 - extract text
        String text = extractTextFromPdf(pdfBytes);
        // Clean text - remove special characters that pollute embeddings
        text = text.replaceAll("[^\\x20-\\x7E\\n\\r\\t]", " ");
        text = text.replaceAll("\\s+", " ").trim();
        System.out.println("Extracted text, length: " + text.length());

       // Step 2b - fallback to Textract for scanned PDFs
        if (text.trim().length() < 500) {
            if (pdfBytes.length > TEXTRACT_MAX_SIZE) {
                throw new RuntimeException(
                        "Scanned PDF is too large (" + (pdfBytes.length / 1024 / 1024) +
                                "MB). Maximum size for scanned PDFs is 10MB. " +
                                "Please compress it using ilovepdf.com before uploading."
                );
            }
            System.out.println("Scanned PDF detected - using AWS Textract OCR");
            text = extractTextWithTextract(event.getS3Key());
        }

        List<String> chunks = splitIntoChunks(text, 500, 50);
        System.out.println("Split into " + chunks.size() + " chunks");

        // parallel embedding
        ExecutorService executor = Executors.newFixedThreadPool(5);

        List<CompletableFuture<Void>> futures = chunks.stream()
                .map(chunk -> CompletableFuture.runAsync(() -> {
                    try {
                        List<Float> vector = generateEmbedding(chunk);
                        storeEmbedding(event.getUserId(), event.getDocId(), chunk, vector);
                    } catch (Exception e) {
                        System.err.println("Failed to embed chunk: " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                }, executor))
                .collect(Collectors.toList());

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            System.err.println("Embedding pipeline failed: " + e.getMessage());
            executor.shutdown();
            updateDocumentStatus(event.getDocId(), "FAILED");
            return;
        }
        executor.shutdown();

        // Publish doc-embedded event for auto-tagging
        String embeddedMessage = String.format(
                "{\"docId\":\"%s\",\"userId\":\"%s\",\"extractedText\":\"%s\"}",
                event.getDocId(),
                event.getUserId(),
                text.replace("\"", "'").replace("\n", " ").substring(0, Math.min(2000, text.length()))
        );
        kafkaTemplate.send(docEmbeddedTopic, event.getDocId(), embeddedMessage);

        System.out.println("All chunks embedded in parallel");
        updateDocumentStatus(event.getDocId(), "READY");
        System.out.println("Document processing complete!");
    }

    public String extractTextWithTextract(String s3Key) {
        DetectDocumentTextRequest request = DetectDocumentTextRequest.builder()
                .document(Document.builder()
                        .s3Object(software.amazon.awssdk.services.textract.model.S3Object.builder()
                                .bucket(bucketName)
                                .name(s3Key)
                                .build())
                        .build())
                .build();

        DetectDocumentTextResponse response = textractClient.detectDocumentText(request);

        return response.blocks().stream()
                .filter(block -> block.blockType() == BlockType.LINE)
                .map(Block::text)
                .collect(Collectors.joining("\n"));
    }
}
