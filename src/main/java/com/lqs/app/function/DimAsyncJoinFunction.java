package com.lqs.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @Author lqs
 * @Date 2022年03月11日 22:53:31
 * @Version 1.0.0
 * @ClassName DimAsyncJoinFunction
 * @Describe
 */
public interface DimAsyncJoinFunction<T> {

    String getKey(T input);

    void join(T input, JSONObject dimInfo) throws ParseException;

}
