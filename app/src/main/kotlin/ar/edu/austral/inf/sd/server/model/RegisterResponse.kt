package ar.edu.austral.inf.sd.server.model

import java.util.Objects
import com.fasterxml.jackson.annotation.JsonProperty
import jakarta.validation.constraints.DecimalMax
import jakarta.validation.constraints.DecimalMin
import jakarta.validation.constraints.Email
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotNull
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Size
import jakarta.validation.Valid

/**
 * 
 * @param nextHost 
 * @param nextPort 
 * @param timeout 
 * @param xGameTimestamp 
 */
data class RegisterResponse(

    @get:JsonProperty("nextHost", required = true) val nextHost: kotlin.String,

    @get:JsonProperty("nextPort", required = true) val nextPort: kotlin.Int,

    @get:Min(0)
    @get:JsonProperty("timeout", required = true) val timeout: kotlin.Int,

    @get:Min(0)
    @get:JsonProperty("xGameTimestamp", required = true) val xGameTimestamp: kotlin.Int
    ) {

}

