package com.lqs.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @Author lqs
 * @Date 2022年03月14日 09:08:51
 * @Version 1.0.0
 * @ClassName TransientSink
 * @Describe 注解
 */


@Target(ElementType.FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
