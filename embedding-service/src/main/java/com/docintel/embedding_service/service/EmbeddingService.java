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
import org.springframework.stereotype.Service;
import dev.langchain4j.model.output.Response;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class EmbeddingService {
    private final DocumentStatusRepository documentStatusRepository;
    private final S3Client s3Client;
    private final EmbeddingModel embeddingModel;

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


    public void storeEmbedding(String userId, String chunk, List<Float> vector){
        EmbeddingStore<TextSegment> store = ChromaEmbeddingStore.builder()
                .baseUrl(chromaUrl)
                .collectionName("embeddings_"+ userId)
                .build();

        Embedding embedding = Embedding.from(vector);
        TextSegment segment = TextSegment.from(chunk);
        store.add(embedding, segment);
    }

    public void updateDocumentStatus(String docId, String status){
        DocumentEntity documentEntity = documentStatusRepository.findById(docId)
                .orElseThrow(() -> new RuntimeException("Document not found"));

        documentEntity.setStatus(status);
        documentStatusRepository.save(documentEntity);
    }

    public void processDocument(DocUploadedEvent event){
        System.out.println("Starting processing for doc: " + event.getDocId());

        // Step 1 - download PDF from S3
        byte[] pdfBytes = downloadFromS3(event.getS3Key());
        System.out.println("Downloaded PDF, size: " + pdfBytes.length);

        // Step 2 - extract text
        String text = extractTextFromPdf(pdfBytes);
        System.out.println("Extracted text, length: " + text.length());

        // Step 3 - split into chunks
        List<String> chunks = splitIntoChunks(text, 500, 50);
        System.out.println("Split into " + chunks.size() + " chunks");

        // Step 4 - for each chunk
        for(int i = 0; i < chunks.size(); i++){
            System.out.println("Processing chunk " + (i+1) + " of " + chunks.size());
            List<Float> vector = generateEmbedding(chunks.get(i));
            storeEmbedding(event.getUserId(), chunks.get(i), vector);
        }

        // Step 5 - update status to READY
        updateDocumentStatus(event.getDocId(), "READY");
        System.out.println("Document processing complete!");
    }
}
