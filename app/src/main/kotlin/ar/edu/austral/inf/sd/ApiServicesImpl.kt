package ar.edu.austral.inf.sd

import ar.edu.austral.inf.sd.server.api.PlayApiService
import ar.edu.austral.inf.sd.server.api.RegisterNodeApiService
import ar.edu.austral.inf.sd.server.api.RelayApiService
import ar.edu.austral.inf.sd.server.api.BadRequestException
import ar.edu.austral.inf.sd.server.api.ReconfigureApiService
import ar.edu.austral.inf.sd.server.api.UnregisterNodeApiService
import ar.edu.austral.inf.sd.server.api.InternalServerErrorException
import ar.edu.austral.inf.sd.server.api.NotFoundException
import ar.edu.austral.inf.sd.server.api.ServiceUnavailableException
import ar.edu.austral.inf.sd.server.api.TimeOutException
import ar.edu.austral.inf.sd.server.model.Node
import ar.edu.austral.inf.sd.server.model.PlayResponse
import ar.edu.austral.inf.sd.server.model.RegisterResponse
import ar.edu.austral.inf.sd.server.model.Signature
import ar.edu.austral.inf.sd.server.model.Signatures
import io.swagger.v3.oas.models.security.SecurityScheme.In
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.getAndUpdate
import kotlinx.coroutines.flow.update
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.HttpServerErrorException.InternalServerError
import org.springframework.web.client.RestClientException
import org.springframework.web.client.RestTemplate
import org.springframework.web.client.postForEntity
import org.springframework.web.context.request.RequestContextHolder
import org.springframework.web.context.request.ServletRequestAttributes
import java.security.MessageDigest
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.random.Random

@Component
class ApiServicesImpl @Autowired constructor(
    private val restTemplate: RestTemplate
): RegisterNodeApiService, RelayApiService, PlayApiService, ReconfigureApiService, UnregisterNodeApiService {

    // Config
    @Value("\${server.name:nada}")
    private val myServerName: String = ""
    @Value("\${server.host:localhost}")
    private val myServerHost: String = "localhost"
    @Value("\${server.port:8080}")
    private val myServerPort: Int = 0
    @Value("\${timeout:20}")
    private val timeout: Int = 20
    @Value("\${register.host:}")
    var registerHost: String = ""
    @Value("\${register.port:-1}")
    var registerPort: Int = -1

    // Coordinator's list of nodes
    private val nodes: MutableList<Node> = mutableListOf()

    // Participant's data
    private var nextNode: RegisterResponse? = null
    private val messageDigest = MessageDigest.getInstance("SHA-512")
    private val mySalt = Base64.getEncoder().encodeToString(Random.nextBytes(9))
    private val myUUID = newUUID()

    private var myTimestamp: Int = 0
    private var nextNodeAfterNextTimestamp: RegisterResponse? = null

    // Current play's data
    private val currentRequest
        get() = (RequestContextHolder.getRequestAttributes() as ServletRequestAttributes).request
    private var resultReady = CountDownLatch(1)
    private var currentMessageWaiting = MutableStateFlow<PlayResponse?>(null)
    private var currentMessageResponse = MutableStateFlow<PlayResponse?>(null)
    private var currentXGameTimestamp = 0

    override fun registerNode(host: String?, port: Int?, uuid: UUID?, salt: String?, name: String?): RegisterResponse {
        Base64.getDecoder().decode(salt)
        val nextNode = if (nodes.isEmpty()) {
            // es el primer nodo
            val me = RegisterResponse(myServerHost, myServerPort, timeout, currentXGameTimestamp)
            val meNode = Node(myServerHost, myServerPort, myServerName, myUUID, mySalt)
            nodes.add(meNode)
            me
        } else {
            val lastNode = nodes.last()
            RegisterResponse(lastNode.host, lastNode.port, timeout, currentXGameTimestamp)
        }
        val node = Node(host!!, port!!, name!!, uuid!!, salt!!)
        nodes.add(node)

        return RegisterResponse(nextNode.nextHost, nextNode.nextPort, timeout, currentXGameTimestamp)
    }

    override fun relayMessage(message: String, signatures: Signatures, xGameTimestamp: Int?): Signature {
        val receivedHash = doHash(message.encodeToByteArray(), mySalt)
        val receivedContentType = currentRequest.getPart("message")?.contentType ?: "nada"
        val receivedLength = message.length
        if (nextNode != null) {
            // Soy un relé. busco el siguiente y lo mando
            val updatedSignatures = signatures.items + clientSign(message, receivedContentType)
            sendRelayMessage(message, receivedContentType, nextNode!!, Signatures(updatedSignatures), xGameTimestamp!!)
        } else {
            // me llego algo, no lo tengo que pasar
            if (currentMessageWaiting.value == null) throw BadRequestException("no waiting message")
            val current = currentMessageWaiting.getAndUpdate { null }!!
            val response = validatePlayResult(current, receivedHash, receivedLength, receivedContentType, signatures)
            currentMessageResponse.update { response }
            currentXGameTimestamp += 1
            resultReady.countDown()
        }
        return Signature(
            name = myServerName,
            hash = receivedHash,
            contentType = receivedContentType,
            contentLength = receivedLength
        )
    }

    override fun play(body: String): PlayResponse {
        if (nodes.isEmpty()) {
            // inicializamos el primer nodo como yo mismo
            val me = Node(myServerHost, myServerPort, myServerName, myUUID, mySalt)

            nodes.add(me)
        }
        currentMessageWaiting.update { newResponse(body) }
        val contentType = currentRequest.contentType

        val expectedSignatures = generateExpectedSignatures(body, nodes, contentType)

        sendRelayMessage(body, contentType, toRegisterResponse(nodes.last(), -1), Signatures(listOf()), currentXGameTimestamp)
        resultReady.await(timeout.toLong(), TimeUnit.SECONDS)
        resultReady = CountDownLatch(1)

        if (currentMessageWaiting.value != null){
            throw TimeOutException("Last relay was not received on time")
        }

        if (!compareSignatures(expectedSignatures, currentMessageResponse.value!!.signatures)){
            throw InternalServerErrorException("Missing signatures")
        }

        if (currentMessageWaiting.value!!.originalHash != currentMessageWaiting.value!!.receivedHash){
            throw ServiceUnavailableException("Received different hash than original")
        }

        return currentMessageResponse.value!!
    }

    private fun generateExpectedSignatures(body: String, nodes: List<Node>, contentType:String): Signatures {

        val signatures = mutableListOf<Signature>()
        for (i in 1..< nodes.size){
            val node = nodes[i]
            val hash = doHash(body.encodeToByteArray(), node.salt)
            signatures.add(Signature(node.name, hash, contentType, body.length))
        }

        return Signatures(signatures)
    }

    private fun compareSignatures(a: Signatures, b: Signatures): Boolean{
        val aSignatureList = a.items
        val bSignatureList = b.items.reversed()

        if (aSignatureList.size != bSignatureList.size) return false

        for (i in aSignatureList.indices) {
            if (aSignatureList[i].hash != bSignatureList[i].hash) return false
        }

        return true
    }

    internal fun registerToServer(registerHost: String, registerPort: Int) {
        val registerUrl = "http://$registerHost:$registerPort/register-node"
        val registerParams = "?host=localhost&port=$myServerPort&name=$myServerName&uuid=$myUUID&salt=$mySalt&name=$myServerName"
        val url = registerUrl + registerParams


        try {
            val response = restTemplate.postForEntity<RegisterResponse>(url)

            val registerNodeResponse: RegisterResponse = response.body!!
            println("nextNode = $registerNodeResponse")
            myTimestamp = registerNodeResponse.xGameTimestamp
            nextNode = with(registerNodeResponse) { RegisterResponse(nextHost, nextPort, timeout, registerNodeResponse.xGameTimestamp) }
        } catch (e: RestClientException){
            print("Could not register to: $registerUrl")
        }
    }

    private fun sendRelayMessage(body: String, contentType: String, relayNode: RegisterResponse, signatures: Signatures, timestamp: Int) {
        if (timestamp < myTimestamp) {
            throw BadRequestException("Invalid timestamp")
        }

        if (nextNodeAfterNextTimestamp != null && timestamp >= nextNodeAfterNextTimestamp!!.xGameTimestamp) {
            myTimestamp = nextNodeAfterNextTimestamp!!.xGameTimestamp
            nextNode = nextNodeAfterNextTimestamp

            nextNodeAfterNextTimestamp = null
        }

        val nextNodeUrl = "http://${relayNode.nextHost}:${relayNode.nextPort}/relay"

        val messageHeaders = HttpHeaders().apply { setContentType(MediaType.parseMediaType(contentType)) }
        val messagePart = HttpEntity(body, messageHeaders)

        val signatureHeaders = HttpHeaders().apply { setContentType(MediaType.APPLICATION_JSON) }
        val signaturesPart = HttpEntity(signatures, signatureHeaders)

        val bodyParts = LinkedMultiValueMap<String, Any>().apply {
            add("message", messagePart)
            add("signatures", signaturesPart)
        }

        val requestHeaders = HttpHeaders().apply {
            setContentType(MediaType.MULTIPART_FORM_DATA)
            add("X-Game-Timestamp", timestamp.toString())
        }
        val request = HttpEntity(bodyParts, requestHeaders)

        try {
            restTemplate.postForEntity<Map<String, Any>>(nextNodeUrl, request)
        } catch (e: RestClientException) {
            // Send failed play to center node
            val hostUrl = "http://${registerHost}:${registerPort}/relay"
            restTemplate.postForEntity<Map<String, Any>>(hostUrl, request)


            throw ServiceUnavailableException("Could not relay message to: $nextNodeUrl")
        }

        myTimestamp = timestamp

    }

    private fun clientSign(message: String, contentType: String): Signature {
        val receivedHash = doHash(message.encodeToByteArray(), mySalt)
        return Signature(myServerName, receivedHash, contentType, message.length)
    }

    private fun newResponse(body: String) = PlayResponse(
        "Unknown",
        currentRequest.contentType,
        body.length,
        doHash(body.encodeToByteArray(), mySalt),
        "Unknown",
        -1,
        "N/A",
        Signatures(listOf())
    )

    private fun doHash(body: ByteArray, salt: String): String {
        val saltBytes = Base64.getDecoder().decode(salt)
        messageDigest.update(saltBytes)
        val digest = messageDigest.digest(body)
        return Base64.getEncoder().encodeToString(digest)
    }

    private fun toRegisterResponse(node: Node, timestamp: Int): RegisterResponse {
        return RegisterResponse(
            node.host,
            node.port,
            timeout,
            timestamp
        )
    }

    private fun validatePlayResult(current: PlayResponse, receivedHash: String, receivedLength: Int, receivedContentType: String, signatures: Signatures): PlayResponse {
        return current.copy(
            contentResult = if (receivedHash == current.originalHash) "Success" else "Failure",
            receivedHash = receivedHash,
            receivedLength = receivedLength,
            receivedContentType = receivedContentType,
            signatures = signatures
        )
    }

    companion object {
        fun newUUID(): UUID = UUID.randomUUID()
    }

    override fun reconfigure(
        uuid: UUID?,
        salt: String?,
        nextHost: String?,
        nextPort: Int?,
        xGameTimestamp: Int?
    ): String {
        if (uuid != myUUID || salt != mySalt){
            throw BadRequestException("Invalid data")
        }

        nextNodeAfterNextTimestamp = RegisterResponse(nextHost!!, nextPort!!, timeout, xGameTimestamp!!)
        return "Reconfigured node $myUUID"
    }

    override fun unregisterNode(uuid: UUID?, salt: String?): String {
        val nodeToUnregister = nodes.find {
            it.uuid == uuid!!
        }

        if (nodeToUnregister == null){
            throw NotFoundException("Node with uuid: $uuid not found")
        }

        if (nodeToUnregister.salt != salt){
             throw BadRequestException("Invalid data")
        }

        val nodeToUnregisterIndex = nodes.indexOf(nodeToUnregister)

        if (nodeToUnregisterIndex < nodes.size - 1){
            val previousNode = nodes[nodeToUnregisterIndex+1]
            val nextNode = nodes[nodeToUnregisterIndex-1]

            val reconfigureUrl = "http://${previousNode.host}:${previousNode.port}/reconfigure"
            val reconfigureParams = "?uuid=${previousNode.uuid}&salt=${previousNode.salt}&nextHost=${nextNode.host}&nextPort=${nextNode.port}"

            val url = reconfigureUrl + reconfigureParams

            val requestHeaders = HttpHeaders().apply {
                add("X-Game-Timestamp", currentXGameTimestamp.toString())
            }
            val request = HttpEntity(requestHeaders.toMap())

            try {
                restTemplate.postForEntity<String>(url, request)
            } catch (e: RestClientException){
                print("Could not reconfigure to: $url")
                throw e
            }
        }

        nodes.removeAt(nodeToUnregisterIndex)
        return "Unregister Successful"
    }

    @PreDestroy
    fun onDestroy(){
        if (registerPort == -1) return // Coordinator just dies

        val unregisterUrl = "http://$registerHost:$registerPort/unregister-node"
        val unregisterParams = "?uuid=$myUUID&salt=$mySalt"
        val url = unregisterUrl + unregisterParams

        try {
            restTemplate.postForEntity<String>(url)
        } catch (e: RestClientException){
            print("Could not unregister to: $url")
            throw e
        }
    }
}