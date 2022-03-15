package com.lqs.app.dws;

import com.lqs.app.function.SplitFunction;
import com.lqs.bean.KeywordStats;
import com.lqs.utils.ClickHouseUtil;
import com.lqs.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年03月15日 16:37:50
 * @Version 1.0.0
 * @ClassName KeywordStatsApp
 * @Describe 关键字主题表
 */

//数据流：web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> ClickHouse
//程  序：mockLog -> Nginx -> Logger.sh  -> Kafka(ZK)  -> BaseLogApp -> kafka -> KeywordStatsApp -> ClickHouse


public class KeywordStatsApp {

    public static void main(String[] args) throws Exception {

        //TODO 1、创建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //在实际开发环境中，应与kafka分区数保持一致
        executionEnvironment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);

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

        //TODO 2、使用DDL方式读取kafka数据创建表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        tableEnvironment.executeSql(
                "create table page_view( " +
                        "    `common` Map<STRING,STRING>, " +
                        "    `page` Map<STRING,STRING>, " +
                        "    `ts` BIGINT, " +
                        "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                        "    WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
                        ") with (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")"
        );

        //TODO 3、过滤数据，上一跳页面为“search” & 搜索词不为空
        Table fullWordTable = tableEnvironment.sqlQuery(
                "" +
                        "select " +
                        "    page['item'] full_word, " +
                        "    rt " +
                        "from  " +
                        "    page_view " +
                        "where " +
                        "    page['last_page_id']='search' and page['item'] is not null"
        );

        //TODO 4、注册udtf，进行分词处理
        tableEnvironment.createTemporarySystemFunction("split_words", SplitFunction.class);
        Table wordTable = tableEnvironment.sqlQuery(
                "" +
                        "SELECT  " +
                        "    word,  " +
                        "    rt " +
                        "FROM  " +
                        "    " + fullWordTable + ", LATERAL TABLE(split_words(full_word))"
        );

        //TODO 5、分组、开窗、聚合
        Table resultTable = tableEnvironment.sqlQuery(
                "" +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +
                "    count(*) ct, " +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from " + wordTable + " " +
                "group by " +
                "    word, " +
                "    TUMBLE(rt, INTERVAL '10' SECOND)"
        );

        //TODO 6、将动态表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnvironment.toAppendStream(resultTable, KeywordStats.class);

        //TODO 7、将数据打印并写入clickHouse
        keywordStatsDataStream.print();
        keywordStatsDataStream.addSink(ClickHouseUtil.getSink("insert into keyword_stats(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //TODO 8、启动任务
        executionEnvironment.execute("KeywordStatsApp");

    }

}
