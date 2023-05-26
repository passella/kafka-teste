package br.com.passella.kafkateste.producer

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service


@Service
class KafkaProducerService(private val kafkaTemplate: KafkaTemplate<String, String>) {

    companion object {
        const val TOPIC_NAME = "test-topic"
    }

    fun sendMessage(message: String) {
        kafkaTemplate.send("test-topic", message)
    }
}
