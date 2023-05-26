package br.com.passella.kafkateste

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaTesteApplication

fun main(args: Array<String>) {
	runApplication<KafkaTesteApplication>(*args)
}
