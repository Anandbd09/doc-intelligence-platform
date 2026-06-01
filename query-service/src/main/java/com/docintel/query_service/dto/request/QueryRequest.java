package com.docintel.query_service.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import jakarta.validation.constraints.NotBlank;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryRequest {

    @NotBlank
    private String userId;
    @NotBlank
    private String question;
}
