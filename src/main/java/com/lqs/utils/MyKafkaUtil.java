package com.lqs.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @Author lqs
 * @Date 2022年03月03日 23:19:49
 * @Version 1.0.0
 * @ClassName MyKafkaUtil
 * @Describe 将流数据推送下游的 Kafka 的 Topic 中，kafka相关工具函数
 */
public class MyKafkaUtil {

    private static String brokers = "nwh120:9092,nwh121:9092,nwh122:9092";
    private static String default_topic = "DWD_DEFAULT_TOPIC";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(
                brokers,
                topic,
                new SimpleStringSchema()
        );
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaProducer<T>(
                default_topic,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }

    //拼接相关kafka相关属性到DDL
    public static String getKafkaDDL(String topic, String groupId) {
        return " 'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + brokers + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset'  ";
    }

}
