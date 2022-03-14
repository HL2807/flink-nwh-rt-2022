package com.lqs.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lqs.app.function.DimAsyncFunction;
import com.lqs.bean.OrderWide;
import com.lqs.bean.PaymentWide;
import com.lqs.bean.ProductStats;
import com.lqs.common.GmallConstant;
import com.lqs.utils.ClickHouseUtil;
import com.lqs.utils.DateTimeUtil;
import com.lqs.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Author lqs
 * @Date 2022年03月14日 16:57:37
 * @Version 1.0.0
 * @ClassName ProductStatsApp
 * @Describe 商品主题宽表的计算
 */

//数据流：
// app/web -> nginx -> SpringBoot -> Kafka(ods) -> FlinkApp -> Kafka(dwd) -> FlinkApp -> ClickHouse
// app/web -> nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Phoenix -> FlinkApp -> Kafka(dwm) -> FlinkApp -> ClickHouse
//程  序： mock -> nginx -> logger.sh -> Kafka(ZK)/Phoenix(HDFS/HBASE/ZK) -> Redis -> ClickHouse

public class ProductStatsApp {

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

        //TODO 2、读取kafka7个主题的数据来创建流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        //点击、曝光
        DataStreamSource<String> pvDS = executionEnvironment.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        //收藏
        DataStreamSource<String> favorDS = executionEnvironment.addSource(MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId));
        //加入购物车
        DataStreamSource<String> cartDS = executionEnvironment.addSource(MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId));
        //下单
        DataStreamSource<String> orderDS = executionEnvironment.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        //支付
        DataStreamSource<String> payDS = executionEnvironment.addSource(MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId));
        //退款
        DataStreamSource<String> refundDS = executionEnvironment.addSource(MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId));
        //评价
        DataStreamSource<String> commentDS = executionEnvironment.addSource(MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId));

        //TODO 3、将7个流统一数据格式

        //点击、曝光
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pvDS.flatMap(
                new FlatMapFunction<String, ProductStats>() {
                    @Override
                    public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                        //将数据转换为JSON对象
                        JSONObject jsonObject = JSON.parseObject(value);
                        //取出page信息
                        JSONObject page = jsonObject.getJSONObject("page");
                        String pageId = page.getString("page_id");
                        Long ts = jsonObject.getLong("ts");

                        if ("good_detail".equals(pageId) && "sku_id".equals(page.getString("item_type"))) {
                            out.collect(
                                    ProductStats.builder()
                                            .sku_id(page.getLong("item"))
                                            .click_ct(1L)
                                            .ts(ts)
                                            .build()
                            );
                        }

                        //尝试取出曝光数据
                        JSONArray displays = jsonObject.getJSONArray("displays");
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                //取出单条曝光数据
                                JSONObject display = displays.getJSONObject(i);

                                if ("sku_id".equals(display.getString("item_type"))) {
                                    out.collect(
                                            ProductStats.builder()
                                                    .sku_id(display.getLong("item"))
                                                    .display_ct(1L)
                                                    .ts(ts)
                                                    .build()
                                    );
                                }
                            }
                        }
                    }
                }
        );

        //收藏
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorDS.map(
                line -> {
                    JSONObject jsonObject = JSON.parseObject(line);
                    return ProductStats.builder()
                            .sku_id(jsonObject.getLong("sku_id"))
                            .favor_ct(1L)
                            .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                            .build();
                }
        );

        //加入购物车
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDataStream = cartDS.map(
                line -> {
                    JSONObject jsonObject = JSON.parseObject(line);
                    return ProductStats.builder()
                            .sku_id(jsonObject.getLong("sku_id"))
                            .cart_ct(1L)
                            .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                            .build();
                }
        );

        //下单
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderDS.map(
                line -> {
                    OrderWide orderWide = JSON.parseObject(line, OrderWide.class);

                    HashSet<Long> orderIds = new HashSet<>();
                    orderIds.add(orderWide.getOrder_id());

                    return ProductStats.builder()
                            .sku_id(orderWide.getSku_id())
                            .order_sku_num(orderWide.getSku_num())
                            .order_amount(orderWide.getSplit_total_amount())
                            .orderIdSet(orderIds)
                            .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                            .build();
                }
        );

        //支付
        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = payDS.map(
                line -> {
                    PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);

                    HashSet<Long> orderIds = new HashSet<>();
                    orderIds.add(paymentWide.getOrder_id());

                    return ProductStats.builder()
                            .sku_id(paymentWide.getSku_id())
                            .payment_amount(paymentWide.getSplit_total_amount())
                            .paidOrderIdSet(orderIds)
                            .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                            .build();
                }
        );

        //退款
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(
                line -> {
                    JSONObject jsonObject = JSON.parseObject(line);

                    HashSet<Long> orderIds = new HashSet<>();
                    orderIds.add(jsonObject.getLong("order_id"));

                    return ProductStats.builder()
                            .sku_id(jsonObject.getLong("sku_id"))
                            .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(orderIds)
                            .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                            .build();
                }
        );

        //评价
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(
                line -> {
                    JSONObject jsonObject = JSON.parseObject(line);

                    String appraise = jsonObject.getString("appraise");
                    long goodCt = 0L;
                    if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                        goodCt = 1L;
                    }

                    return ProductStats.builder()
                            .sku_id(jsonObject.getLong("sku_id"))
                            .comment_ct(1L)
                            .good_comment_ct(goodCt)
                            .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                            .build();
                }
        );

        //TODO 4、union 7个流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(
                productStatsWithFavorDS,
                productStatsWithCartDataStream,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS
        );

        //TODO 5、提取时间戳生成watermark
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                            @Override
                            public long extractTimestamp(ProductStats element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        //TODO 6、分组、开窗、聚合，按照sku_id分组，10秒滚动窗口，结合增量聚合（累加值）和全量聚合（提取窗口信息）
        SingleOutputStreamOperator<ProductStats> reduceDS = productStatsWithWatermarkDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats value1, ProductStats value2) throws Exception {
                                value1.setDisplay_ct(value1.getDisplay_ct() + value2.getDisplay_ct());
                                value1.setClick_ct(value1.getClick_ct() + value2.getClick_ct());
                                value1.setCart_ct(value1.getCart_ct() + value2.getCart_ct());
                                value1.setFavor_ct(value1.getFavor_ct() + value2.getFavor_ct());
                                value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                //value1.setOrder_ct(value1.getOrderIdSet().size()+0L);
                                value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());
                                value1.setPayment_amount(value1.getPayment_amount().add(value2.getPayment_amount()));

                                value1.getRefundOrderIdSet().addAll(value2.getRefundOrderIdSet());
                                //value1.setRefund_order_ct(value1.getRefundOrderIdSet().size()+0L);
                                value1.setRefund_amount(value1.getRefund_amount().add(value2.getRefund_amount()));

                                value1.getPaidOrderIdSet().addAll(value2.getPaidOrderIdSet());
                                //value1.setPaid_order_ct(value1.getPaidOrderIdSet().size()+0L);

                                value1.setComment_ct(value1.getComment_ct() + value2.getComment_ct());
                                value1.setGood_comment_ct(value1.getGood_comment_ct() + value2.getGood_comment_ct());

                                return value1;
                            }
                        }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            @Override
                            public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                                //取出数据
                                ProductStats productStats = input.iterator().next();

                                //设置窗口时间
                                productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                                productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                                //设置订单数量
                                productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                                productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                                productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                                //将数据写出
                                out.collect(productStats);
                            }
                        }
                );

        //TODO 7、关联维度信息

        //TODO 7.1、关联sku维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(ProductStats input) {
                        return input.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setSku_name(dimInfo.getString("SKU_NAME"));
                        input.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        input.setSpu_id(dimInfo.getLong("SPU_ID"));
                        input.setTm_id(dimInfo.getLong("TM_ID"));
                        input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 7.2、关联spu维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(
                productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(ProductStats input) {
                        return String.valueOf(input.getSpu_id());
                    }

                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 7.3、关联category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(
                productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setCategory3_name(dimInfo.getString("NAME"));
                    }

                    @Override
                    public String getKey(ProductStats input) {
                        return String.valueOf(input.getCategory3_id());
                    }

                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 7.4、关联tm维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS = AsyncDataStream.unorderedWait(
                productStatsWithCategory3DS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setTm_name(dimInfo.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(ProductStats input) {
                        return String.valueOf(input.getTm_id());
                    }

                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 8、将数据写入clickHouse
        productStatsWithTmDS.print();
        productStatsWithTmDS.addSink(
                ClickHouseUtil.getSink("insert into table product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        //TODO 9、启动任务
        executionEnvironment.execute("ProductStatsApp");

    }

}
