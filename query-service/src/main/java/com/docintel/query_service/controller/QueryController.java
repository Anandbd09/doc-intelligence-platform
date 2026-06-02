package com.docintel.query_service.controller;

import com.docintel.query_service.dto.request.CompareRequest;
import com.docintel.query_service.dto.request.CrossSearchRequest;
import com.docintel.query_service.dto.request.QueryRequest;
import com.docintel.query_service.dto.request.SummarizeRequest;
import com.docintel.query_service.dto.response.CrossSearchResponse;
import com.docintel.query_service.dto.response.QueryResponse;
import com.docintel.query_service.service.QueryService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/query")
@RequiredArgsConstructor
public class QueryController {
    private final QueryService queryService;
    private final RedisTemplate<String, String> redisTemplate;

    @PostMapping
    public ResponseEntity<QueryResponse> queryResponse(
            @RequestBody @Valid QueryRequest request
            ){
        return ResponseEntity.ok(queryService.answerQuestion(request.getUserId(),request.getDocId(), request.getQuestion()));
    }

    @PostMapping("/summarize")
    public ResponseEntity<QueryResponse> summarize(
            @RequestBody @Valid SummarizeRequest request
    ) {
        return ResponseEntity.ok(queryService.summarizeDocument(
                request.getUserId(),
                request.getDocId()
        ));
    }

    @PostMapping("/search")
    public ResponseEntity<CrossSearchResponse> crossSearch(
            @RequestBody @Valid CrossSearchRequest request
    ) {
        return ResponseEntity.ok(queryService.crossSearch(
                request.getUserId(),
                request.getQuestion()
        ));
    }

    @PostMapping("/compare")
    public ResponseEntity<QueryResponse> compareDocuments(
            @RequestBody @Valid CompareRequest request
    ) {
        return ResponseEntity.ok(queryService.compareDocuments(
                request.getUserId(),
                request.getDocId1(),
                request.getDocId2(),
                request.getQuestion()
        ));
    }

    @GetMapping("/history/{userId}/{docId}")
    public ResponseEntity<List<String>> getHistory(
            @PathVariable String userId,
            @PathVariable String docId
    ) {
        String historyKey = "chat_" + userId + "_" + docId;
        List<String> history = redisTemplate.opsForList().range(historyKey, 0, -1);
        return ResponseEntity.ok(history != null ? history : new ArrayList<>());
    }
}
