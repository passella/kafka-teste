package br.com.passella.kafkateste.controller

import br.com.passella.kafkateste.model.Message
import br.com.passella.kafkateste.producer.KafkaProducerService
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/messages")
class MessageController (private val kafkaProducerService: KafkaProducerService){

    @PostMapping("/")
    @ResponseStatus(HttpStatus.CREATED)
    fun sendMessage(@RequestBody message: Message) {
        kafkaProducerService.sendMessage(message.content)
    }
}
