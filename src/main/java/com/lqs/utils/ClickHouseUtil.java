package com.lqs.utils;

import com.lqs.bean.TransientSink;
import com.lqs.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author lqs
 * @Date 2022年03月14日 09:00:37
 * @Version 1.0.0
 * @ClassName ClickHouseUtil
 * @Describe clickHouse JDBC工具类
 */
public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSink(String sql) {

        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        try {
                            //获取所有的属性信息
                            Field[] fields = t.getClass().getDeclaredFields();
                            //遍历字段fields
                            int offset = 0;//偏移量
                            for (int i = 0; i < fields.length; i++) {
                                //获取字段
                                Field field = fields[i];
                                //设置私有属性可访问
                                field.setAccessible(true);
                                //获取字段上注解
                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if (annotation != null) {
                                    //存在该注解,就退出当前循环
                                    offset++;
                                    continue;
                                }
                                //获取值
                                Object value = field.get(t);
                                //给预编译SQL对象赋值
                                preparedStatement.setObject(i + 1 - offset, value);
                            }
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );

    }

}
