package br.com.passella.kafkateste.config

import br.com.passella.kafkateste.exception.KafkaCommonErrorHandlerFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ContainerProperties


@Configuration
@EnableKafka
class KafkaConfig {

    companion object {
        private const val HOST = "localhost:29092"
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(producerFactory())
    }

    @Bean
    fun producerFactory(): DefaultKafkaProducerFactory<String, String> {
        val configProps: MutableMap<String, Any> = HashMap()
        configProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = HOST
        configProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        configProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        return DefaultKafkaProducerFactory(configProps)
    }

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactory()
        factory.setConcurrency(Runtime.getRuntime().availableProcessors())
        factory.setCommonErrorHandler(KafkaCommonErrorHandlerFactory.createDefaultErrorHandler())
        factory.containerProperties.ackMode = ContainerProperties.AckMode.RECORD
        factory.setRecordInterceptor(RecordInterceptorFactory.createDefaultRecordInterceptor())
        return factory
    }

    @Bean
    fun consumerFactory(): DefaultKafkaConsumerFactory<String, String> {
        val configProps: MutableMap<String, Any> = HashMap()
        configProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = HOST
        configProps[ConsumerConfig.GROUP_ID_CONFIG] = "test-group"
        configProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        configProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            StringDeserializer::class.java
        configProps[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = Runtime.getRuntime().availableProcessors()
        return DefaultKafkaConsumerFactory(configProps)
    }


}
