package com.docintel.query_service.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.model.chat.ChatLanguageModel;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class AutoTaggingConsumer {

    private final ChatLanguageModel chatLanguageModel;
    private final MongoTemplate mongoTemplate;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${app.kafka.topic.doc-embedded}", groupId = "query-service-group")
    public void consumeDocEmbedded(String message) {
        try {
            Map<?, ?> event = objectMapper.readValue(message, Map.class);
            String docId = (String) event.get("docId");
            String extractedText = (String) event.get("extractedText");

            System.out.println("Auto-tagging document: " + docId);

            // Generate tags using LLM
            String prompt = """
                Analyze the following document content and generate 3-5 relevant tags/categories.
                
                Rules:
                - Return ONLY a comma-separated list of tags
                - Tags should be single words or short phrases
                - Tags should describe the document type and main topics
                - Example output: Java, Programming, Interview, Technical, Backend
                - No explanations, no bullet points, just comma-separated tags
                
                Document content:
                %s
                
                Tags:
                """.formatted(extractedText);

            String response = chatLanguageModel.generate(prompt);

            // Parse tags from response
            List<String> tags = Arrays.stream(response.split(","))
                    .map(String::trim)
                    .filter(tag -> !tag.isEmpty())
                    .limit(5)
                    .collect(Collectors.toList());

            System.out.println("Generated tags: " + tags);

            // Save tags to MongoDB
            Query query = new Query(Criteria.where("_id").is(docId));
            Update update = new Update().set("tags", tags);
            mongoTemplate.updateFirst(query, update, "documents");

            System.out.println("Tags saved for document: " + docId);

        } catch (Exception e) {
            System.err.println("Auto-tagging failed: " + e.getMessage());
        }
    }
}