package ar.edu.austral.inf.sd.server.api


interface UnregisterNodeApiService {

    /**
     * POST /unregister-node
     * Un participante decide dejar el juego
     *
     * @param uuid  (optional)
     * @param salt  (optional)
     * @return Sashay Away (status code 202)
     *         or User error, replace user. (status code 400)
     * @see UnregisterNodeApi#unregisterNode
     */
    fun unregisterNode(uuid: java.util.UUID?, salt: kotlin.String?): kotlin.String
}
