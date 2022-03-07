package com.lqs.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.lqs.app.function.CustomerDeserialization;
import com.lqs.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lqs
 * @Date 2022年03月04日 09:59:08
 * @Version 1.0.0
 * @ClassName FlinkCDC
 * @Describe 业务数据采集层，即ods层业务数据采集，通过FlinkCDC
 */
public class FlinkCDC {
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

        //2、通过flinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("nwh120")
                .port(3306)
                .username("root")
                .password("912811")
                .databaseList("gmall2022")
                //.tableList("gmall2022.base_trademark") //如果不添加该参数，则消费指定数据库中所有表的数据，如果指定，指定方式为db.table
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> streamSource = executionEnvironment.addSource(sourceFunction);

        //打印数据并将数据写入kafka
        streamSource.print();
        String sinkTopic="ods_base_db";
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //启动任务
        executionEnvironment.execute("FlinkCDC");

    }
}
