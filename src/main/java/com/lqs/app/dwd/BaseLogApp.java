package com.lqs.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lqs.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author lqs
 * @Date 2022年03月04日 20:20:07
 * @Version 1.0.0
 * @ClassName BaseLogApp
 * @Describe 日志dwd层
 */

//数据流：web/app -> Nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd)
//程  序：mockLog -> Nginx -> Logger.sh  -> Kafka(ZK)  -> BaseLogApp -> kafka

public class BaseLogApp {

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

        //消费ods_base_log主题数据创建流
        String sourceTopic="ods_base_log";
        String groupId="base_log_app";
        DataStreamSource<String> kafkaDs = executionEnvironment.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        
        //将每行数据转换为JSON对象
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDs.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    //发生异常，将数据写入侧输出流
                    ctx.output(outputTag, value);
                }
            }
        });

        //打印脏数据
        jsonObjDs.getSideOutput(outputTag).print("Dirty>>>>>>>>>>>>>");

        //新老用户校验，使用状态编程来解决
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {

                        //获取数据中国的"is_new"标记
                        String isNew = value.getJSONObject("common").getString("is_new");

                        //判断isNew标记是否为“1”
                        if ("1".equals(isNew)) {

                            //获取状态数据
                            String state = valueState.value();

                            if (state != null) {
                                //修改isNew标记为0
                                value.getJSONObject("common").put("is_new", "0");
                            } else {
                                valueState.update("1");
                            }
                            //state!=null?value.getJSONObject("common").put("is_new","0"):valueState.update("1");
                        }
                        return value;
                    }
                });
        //分流 侧输出流 页面：主流、启动：侧输出流、曝光：侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start"){
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {

                //后期启动日志字段
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    //将数据写入启动日志侧输出流
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //将数据写入启动日志侧输出流
                    out.collect(value.toJSONString());

                    //取出数据写入页面日志主流
                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {
                        //获取页面ID
                        String pageId = value.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);

                            //添加页面id
                            display.put("page_id", pageId);
                            //将输出写出到曝光侧输出流
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                }

            }
        });

        //提取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        //将三个流进行打印并输出到对应的kafka主题中
        startDS.print("start>>>>>>>>>>>>>");
        pageDS.print("page>>>>>>>>>>>>>>");
        displayDS.print("display>>>>>>>>>>>");

        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        //启动任务
        executionEnvironment.execute("BaseLogApp");

    }

}
