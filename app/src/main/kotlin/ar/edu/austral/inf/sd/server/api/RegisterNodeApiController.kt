package ar.edu.austral.inf.sd.server.api

import ar.edu.austral.inf.sd.server.model.RegisterResponse
import jakarta.validation.Valid
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.*

@RestController
@Validated
@RequestMapping("\${api.base-path:}")
class RegisterNodeApiController(@Autowired(required = true) val service: RegisterNodeApiService) {


    @RequestMapping(
        method = [RequestMethod.POST],
        value = ["/register-node"],
        produces = ["application/json"]
    )
    fun registerNode( @Valid @RequestParam(value = "host", required = false) host: kotlin.String?, @Valid @RequestParam(value = "port", required = false) port: kotlin.Int?, @Valid @RequestParam(value = "uuid", required = false) uuid: java.util.UUID?, @Valid @RequestParam(value = "salt", required = false) salt: kotlin.String?, @Valid @RequestParam(value = "name", required = false) name: kotlin.String?): ResponseEntity<RegisterResponse> {
        return service.registerNode(host, port, uuid, salt, name)
    }
}
