package com.lqs.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.bean.VisitorStats;
import com.lqs.utils.ClickHouseUtil;
import com.lqs.utils.DateTimeUtil;
import com.lqs.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.time.Duration;
import java.util.Date;

/**
 * @Author lqs
 * @Date 2022年03月13日 11:39:09
 * @Version 1.0.0
 * @ClassName VisitorStatsApp
 * @Describe 访客主题宽表的计算
 */
//数据流：web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> Kafka(dwm) -> FlinkApp -> ClickHouse
//程  序：mockLog -> Nginx -> Logger.sh  -> Kafka(ZK)  -> BaseLogApp -> kafka -> uv/uj -> Kafka -> VisitorStatsApp -> ClickHouse

public class VisitorStatsApp {

    public static void main(String[] args) throws Exception {

        //TODO 1、创建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //在实际开发环境中，应与kafka分区数保持一致
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

        //TODO 2、读取kafka数据创建流
        String groupId = "visitor_stats_app";
        String uniqueVisitSourceTopic = "dwm_unique_visit"; //独立访问，即日活
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String pageViewSourceTopic = "dwd_page_log";
        DataStreamSource<String> uvDS = executionEnvironment.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = executionEnvironment.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pvDS = executionEnvironment.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

        //TODO 3、将每个流处理成相同的数据类型
        //TODO 3.1、处理uv数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(
                line -> {
                    JSONObject jsonObject = JSON.parseObject(line);
                    //提取公共字段
                    JSONObject common = jsonObject.getJSONObject("common");
                    return new VisitorStats(",", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            1L, 0L, 0L, 0L, 0L,
                            jsonObject.getLong("ts")
                    );
                }
        );

        //TODO 3.2、梳理uj数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(
                line -> {
                    JSONObject jsonObject = JSON.parseObject(line);
                    //提取公共字段
                    JSONObject common = jsonObject.getJSONObject("common");
                    return new VisitorStats("", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            0L, 0L, 0L, 1L, 0L,
                            jsonObject.getLong("ts")
                    );
                }
        );

        //TODO 3.3、梳理pv数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDS = pvDS.map(
                line -> {
                    JSONObject jsonObject = JSON.parseObject(line);
                    JSONObject common = jsonObject.getJSONObject("common");
                    //获取页面信息
                    JSONObject page = jsonObject.getJSONObject("page");
                    String last_page_id = page.getString("last_page_id");
                    Long sv = 0L;

                    if (last_page_id == null || last_page_id.length() <= 0) {
                        sv = 1L;
                    }

                    return new VisitorStats("", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            0L, 1L, sv, 0L, page.getLong("during_time"),
                            jsonObject.getLong("ts"));
                }
        );

        //TODO 4、union uv、uj、pv三个流
        DataStream<VisitorStats> unionDS = visitorStatsWithUvDS.union(
                visitorStatsWithUjDS,
                visitorStatsWithPvDS
        );

        //TODO 5、提取时间戳生成watermark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );

        //TODO 6、按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWMDS.keyBy(
                new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                        return new Tuple4<String, String, String, String>(
                                value.getAr(),
                                value.getCh(),
                                value.getIs_new(),
                                value.getVc()
                        );
                    }
                }
        );

        //TODO 7、开窗聚合，10s滑动一次窗口
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window (TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                        value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                        value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                        value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                        value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                        value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());

                        return value1;
                    }
                },
                new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                        long start = window.getStart();
                        long end = window.getEnd();

                        VisitorStats visitorStats = input.iterator().next();

                        //补充窗口的信息
                        visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                        visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));

                        out.collect(visitorStats);
                    }
                }
        );

        //TODO 8、将数据写入进CLickHouse
        result.print(">>>>>>>>>>>>>>");
        result.addSink(ClickHouseUtil.getSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9、启动任务
        executionEnvironment.execute("VisitorStatsApp");

    }

}
