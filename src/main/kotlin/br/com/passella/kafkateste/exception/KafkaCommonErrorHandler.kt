package br.com.passella.kafkateste.exception

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries
import java.util.logging.Logger


class KafkaCommonErrorHandlerFactory {
    companion object {
        private val logger = Logger.getLogger(KafkaCommonErrorHandlerFactory::class.java.name)

        fun createDefaultErrorHandler(): CommonErrorHandler {
            val backOff = ExponentialBackOffWithMaxRetries(2)
            return DefaultErrorHandler({ consumerRecord: ConsumerRecord<*, *>, exception: Exception ->
                logger.severe("Excedeu todas as tentativas! $exception $consumerRecord")
            }, backOff)
        }
    }

}
