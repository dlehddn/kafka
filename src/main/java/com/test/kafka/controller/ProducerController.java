package com.test.kafka.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@Slf4j
@RequiredArgsConstructor
@RequestMapping("kafka")
public class ProducerController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final NewTopic myTopic1;
    private final NewTopic myTopic2;

    @GetMapping("/publish/mytopic1")
    public String publishSpringTopic1() {

        String message = String.valueOf(UUID.randomUUID());

        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(myTopic1.name(), message);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                log.info("Unable to send message = {} due to : {}", message, ex.getMessage());
            }
            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("Sent message = {} with offset : {}", message, result.getRecordMetadata().offset());
            }
        });
        return "done";
    }

}
