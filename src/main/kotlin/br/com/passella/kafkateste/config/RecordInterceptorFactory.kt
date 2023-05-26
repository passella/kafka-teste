package br.com.passella.kafkateste.config

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.listener.RecordInterceptor
import java.util.logging.Logger

class RecordInterceptorFactory() {
    companion object {
        private val logger = Logger.getLogger(RecordInterceptorFactory::class.java.name)


        private fun getRemainingMessagesInTopic(bootstrapServers: String, groupId: String, topic: String): Long {
            val adminClient = AdminClient.create(mapOf("bootstrap.servers" to bootstrapServers))

            val consumer = KafkaConsumer<String, String>(
                mapOf(
                    "bootstrap.servers" to bootstrapServers,
                    "group.id" to groupId,
                    "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
                    "value.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer"
                )
            )

            val topicPartitions = consumer.partitionsFor(topic)
                .map { TopicPartition(it.topic(), it.partition()) }
                .toSet()

            val offsetsOptions = ListConsumerGroupOffsetsOptions()
                .topicPartitions(topicPartitions.toMutableList())

            val offsetsResult: ListConsumerGroupOffsetsResult =
                adminClient.listConsumerGroupOffsets(groupId, offsetsOptions)

            val remainingOffsets = offsetsResult.partitionsToOffsetAndMetadata().get()

            val latestOffsets = consumer.endOffsets(topicPartitions)

            var remainingMessages = 0L

            remainingOffsets.forEach { (topicPartition, offsetAndMetadata) ->
                val latestOffset = latestOffsets[topicPartition]
                val committedOffset = offsetAndMetadata.offset()
                if (latestOffset != null) {
                    val remaining = latestOffset - committedOffset
                    remainingMessages += remaining
                }
            }

            adminClient.close()
            consumer.close()

            return remainingMessages
        }


        fun createDefaultRecordInterceptor(): RecordInterceptor<String, String> {
            return object : RecordInterceptor<String, String> {

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
                ): ConsumerRecord<String, String> {

                    val remainingMessagesInTopic = getRemainingMessagesInTopic(
                        "localhost:29092",
                        consumer.groupMetadata().groupId(),
                        record.topic()
                    )

                    logger.info("Mensagens restantes: $remainingMessagesInTopic")

                    return record
                }

            }
        }
    }

}
