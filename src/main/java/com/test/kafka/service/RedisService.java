package com.test.kafka.service;

import com.test.kafka.domain.RankingDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Transactional
@RequiredArgsConstructor
@Slf4j
public class RedisService {

    private final RedisTemplate redisTemplate;

    public void addScore(String boardId) {
        redisTemplate.opsForZSet().incrementScore("ranking", boardId, 1);
    }

    public List<RankingDto> getRankingList(int count) {
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        Set<ZSetOperations.TypedTuple<String>> ranking = zSetOperations.reverseRangeWithScores("ranking", 0, count - 1);
        return ranking.stream()
                .map(tuple -> new RankingDto(tuple.getValue(), tuple.getScore()))
                .collect(Collectors.toList());
    }

}
