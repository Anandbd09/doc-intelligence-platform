package com.docintel.query_service.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor@NoArgsConstructor
public class QueryResponse {
    private String answer;
    List<String> sources;
}
