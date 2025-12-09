package org.jetbrains.api.model

import com.fasterxml.jackson.annotation.JsonProperty
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Positive

data class CatResponse(
    @Positive
    @JsonProperty("id", required = true)
    val id: Long,
    @NotBlank
    @JsonProperty("name", required = true)
    val name: String,
    @NotBlank
    @JsonProperty("breed", required = true)
    val breed: String
)