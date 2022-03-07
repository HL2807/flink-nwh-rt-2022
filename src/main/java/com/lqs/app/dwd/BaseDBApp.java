package com.lqs.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
    public static void main(String[] args) {

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

    }
}
