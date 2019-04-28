package com.windy.starter.mq.annotation;

import java.lang.annotation.*;

/**
 * @Author: windy
 * @Date: 2019-04-25 10:41
 * @Version 1.0
 * @mail nightwindy163@gmail.com
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MQKey {
    String prefix() default "";
}
