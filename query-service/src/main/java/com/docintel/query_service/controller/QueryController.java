package com.docintel.query_service.controller;

import com.docintel.query_service.dto.request.QueryRequest;
import com.docintel.query_service.dto.response.QueryResponse;
import com.docintel.query_service.service.QueryService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/query")
@RequiredArgsConstructor
public class QueryController {
    private final QueryService queryService;

    @PostMapping
    public ResponseEntity<QueryResponse> queryResponse(
            @RequestBody @Valid QueryRequest request
            ){
        return ResponseEntity.ok(queryService.answerQuestion(request.getUserId(), request.getQuestion()));
    }

}
