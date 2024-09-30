package ar.edu.austral.inf.sd.server.api

import ar.edu.austral.inf.sd.server.model.PlayResponse

interface PlayApiService {

    /**
     * POST /play
     * Comienza el juego!
     *
     * @param body El mensaje a enviar por la red telefónica (required)
     * @return La red telefónica funcionó bien! (status code 200)
     *         or Juego cerrado. Se llevaron la pelota. (status code 400)
     *         or Faltan firmas. (status code 500)
     *         or Yo mandé peras y recibí bananas (status code 503)
     *         or La red telefónica falló, no me contestaron. (status code 504)
     * @see PlayApi#play
     */
    fun play(body: kotlin.String): PlayResponse
}
