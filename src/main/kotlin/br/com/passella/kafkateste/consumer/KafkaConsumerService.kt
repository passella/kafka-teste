package br.com.passella.kafkateste.consumer


import br.com.passella.kafkateste.exceptions.ProcessamentoException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.logging.Logger


@Component
class KafkaConsumerService {

    companion object {
        private const val TOPIC_NAME = "test-topic"
        private val logger = Logger.getLogger(KafkaConsumerService::class.java.name)
    }

    @KafkaListener(topics = [TOPIC_NAME], groupId = "test-group")
    fun consumeMessage(message: ConsumerRecord<String, String>) {
        logger.info("Mensagem recebida: {$message.value()}")

        if (message.value() == "error") {
            throw ProcessamentoException("Erro ao processar!")
        }

        logger.info("Mensagem processada com sucesso")
    }

}
