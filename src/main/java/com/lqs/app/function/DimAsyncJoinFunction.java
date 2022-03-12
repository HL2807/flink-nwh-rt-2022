package com.lqs.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @Author lqs
 * @Date 2022年03月11日 22:53:31
 * @Version 1.0.0
 * @ClassName DimAsyncJoinFunction
 * @Describe 优化：异步查询：自定义维度查询接口
 * 这个异步维表查询的方法适用于各种维表的查询，用什么条件查，查出来的结果如何合并到数据流对象中，需要使用者自己定义。
 */
public interface DimAsyncJoinFunction<T> {

    String getKey(T input);

    /**
     * 实现维度表的join
     * @param input 传入实体类
     * @param dimInfo 传入dim层的维度表的数据对象
     * @throws ParseException
     */
    void join(T input, JSONObject dimInfo) throws ParseException;

}
