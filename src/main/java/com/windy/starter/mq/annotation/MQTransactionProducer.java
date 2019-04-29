package com.windy.starter.mq.annotation;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * @Author: windy
 * @Date: 2019-04-25 10:41
 * @Version 1.0
 * @mail nightwindy163@gmail.com
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface MQTransactionProducer {

    /**
     * *重要* 事务的反查是基于同一个producerGroup为维度
     */
    String producerGroup();
}
