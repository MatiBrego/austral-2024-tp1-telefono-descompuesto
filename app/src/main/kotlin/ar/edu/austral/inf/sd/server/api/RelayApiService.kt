package ar.edu.austral.inf.sd.server.api

import ar.edu.austral.inf.sd.server.model.Signature
import ar.edu.austral.inf.sd.server.model.Signatures

interface RelayApiService {

    /**
     * POST /relay
     * Firma un mensaje y lo manda al siguiente
     *
     * @param message  (required)
     * @param signatures  (required)
     * @param xGameTimestamp  (optional)
     * @return mensaje recibido y reenviado. (status code 200)
     *         or mensaje recibido, pero no te digo si lo envié o nó. (status code 202)
     *         or Tu reloj atrasa... (status code 400)
     *         or Los bits se perdieron en el multiverso (status code 503)
     * @see RelayApi#relayMessage
     */
    fun relayMessage(message: kotlin.String, signatures: Signatures, xGameTimestamp: kotlin.Int?): Signature
}
