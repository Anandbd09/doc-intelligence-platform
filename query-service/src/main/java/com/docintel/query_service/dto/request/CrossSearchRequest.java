package com.docintel.query_service.dto.request;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CrossSearchRequest {

    @NotBlank
    private String userId;
    @NotBlank
    private String question;
}
