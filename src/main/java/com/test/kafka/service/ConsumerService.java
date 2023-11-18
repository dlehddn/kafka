package com.test.kafka.service;

import com.test.kafka.domain.RankingDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;

@RequiredArgsConstructor
@Service
@Slf4j
public class ConsumerService {


    private final RedisService redisService;

    @KafkaListener(topics = "#{myTopic1.name}", groupId = "group1")
    public void consumeMyTopic1(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        log.info("[consume message]: {} from partition: {}", message, partition);
        redisService.addScore("게시물2번");
        List<RankingDto> list = redisService.getRankingList(5);
        for (RankingDto a : list) {
            System.out.print("score = " + a.getScore() + ", ");
            System.out.println("boardId = " + a.getBoardId());
        }
    }

}
