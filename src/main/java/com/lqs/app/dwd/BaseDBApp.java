package com.lqs.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.lqs.app.function.CustomerDeserialization;
import com.lqs.app.function.DimSinkFunction;
import com.lqs.app.function.TableProcessFunction;
import com.lqs.bean.TableProcess;
import com.lqs.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @Author lqs
 * @Date 2022年03月05日 10:10:59
 * @Version 1.0.0
 * @ClassName BaseDBApp
 * @Describe 实时数仓dwd层业务数据层
 */

//数据流：web/app -> nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka(dwd)/Phoenix(dim)
//程  序：           mockDb -> Mysql -> FlinkCDC -> Kafka(ZK) -> BaseDBApp -> Kafka/Phoenix(hbase,zk,hdfs)

public class BaseDBApp {
    public static void main(String[] args) throws Exception {

        //TODO 创建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        //1.1 开启CK并指定状态后端为FS memory fs rocksdb
        //executionEnvironment.setStateBackend(new FsStateBackend("hdfs://nwh120:8020/gmall2022/ck"));
        //
        //executionEnvironment.enableCheckpointing(5000L); //5second
        //executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); //设置模式
        //executionEnvironment.getCheckpointConfig().setCheckpointTimeout(10000L);
        //executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(2); //最多允许存在多少个检查点
        //executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000); //两次ck之间暂停多少时间
        //
        ////老版本需要设置
        //executionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        //消费kafka ods_base_db 的主题数据
        String sourceTopic="ods_base_db";
        String groupId="base_db_app";
        DataStreamSource<String> kafkaDS = executionEnvironment.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //将每行数据转换为JSON对象并过滤（delete）主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //取出数据的操作类型
                        String type = value.getString("type");

                        return !"delete".equals(type);
                    }
                });

        //使用flinkCDC消费配置表并处理成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("nwh120")
                .port(3306)
                .username("root")
                .password("912811")
                .databaseList("gmall2022_rt")
                .tableList("gmall2022_rt.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> tableProcessStrDS = executionEnvironment.addSource(sourceFunction);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state", String.class, TableProcess.class);
        //创建广播流
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

        //连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        //分流处理数据，广播流数据，主流数据（根据广播数据进行处理）
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        //提取kafka流数据和HBase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);

        //将kafka数据写入kafka主题，将hbase数据写入Phoenix表
        kafka.print("kafka>>>>>>>>>>");
        hbase.print("hbase>>>>>>>>>>");

        hbase.addSink(new DimSinkFunction());
        kafka.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(element.getString("sinkTable"),
                        element.getString("after").getBytes());
            }
        }));

        //启动任务
        executionEnvironment.execute("BaseDBApp");

    }
}
