package br.com.passella.kafkateste.config

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.listener.RecordInterceptor
import java.util.logging.Logger

class RecordInterceptorFactory {
    companion object {
        private val logger = Logger.getLogger(RecordInterceptorFactory::class.java.name)
        fun createDefaultRecordInterceptor(): RecordInterceptor<String, String> {
            return object : RecordInterceptor<String, String >{

                override fun failure(
                    record: ConsumerRecord<String, String>,
                    exception: Exception,
                    consumer: Consumer<String, String>
                ) {
                    logger.severe("Erro ao consumir mensagem: {$record}")
                }

                override fun intercept(
                    record: ConsumerRecord<String, String>,
                    consumer: Consumer<String, String>
                ): ConsumerRecord<String, String>? {
                    return record
                }

            }
        }
    }

}
