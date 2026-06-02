package com.docintel.query_service.dto.request;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor@NoArgsConstructor
public class CompareRequest {
    @NotBlank
    private String userId;
    @NotBlank
    private String docId1;
    @NotBlank
    private String docId2;
    @NotBlank
    private String question;
}
