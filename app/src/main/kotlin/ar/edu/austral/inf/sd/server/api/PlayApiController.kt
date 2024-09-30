package ar.edu.austral.inf.sd.server.api

import ar.edu.austral.inf.sd.server.model.PlayResponse
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity

import org.springframework.web.bind.annotation.*
import org.springframework.validation.annotation.Validated
import org.springframework.beans.factory.annotation.Autowired

import jakarta.validation.Valid

@RestController
@Validated
@RequestMapping("\${api.base-path:}")
class PlayApiController(@Autowired(required = true) val service: PlayApiService) {


    @RequestMapping(
        method = [RequestMethod.POST],
        value = ["/play"],
        produces = ["application/json"]
    )
    fun sendMessage( @Valid @RequestBody body: kotlin.String): ResponseEntity<PlayResponse> {
        return ResponseEntity(service.play(body), HttpStatus.valueOf(200))
    }
}
