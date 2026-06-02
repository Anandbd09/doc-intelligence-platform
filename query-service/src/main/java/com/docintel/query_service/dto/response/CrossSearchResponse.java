package com.docintel.query_service.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CrossSearchResponse {
    private String answer;
    private List<DocumentMatch> documentMatches;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DocumentMatch{
        private String docId;
        private String docName;
        private List<String> relevantChunks;
    }
}
