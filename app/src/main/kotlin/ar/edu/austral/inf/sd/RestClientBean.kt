package ar.edu.austral.inf.sd

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.client.RestTemplate

@Configuration
open class RestClientBean {
    @Bean
    open fun restTemplate(): RestTemplate {
        return RestTemplate()
    }
}