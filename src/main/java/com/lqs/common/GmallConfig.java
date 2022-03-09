package com.lqs.common;

/**
 * @Author lqs
 * @Date 2022年03月09日 16:05:43
 * @Version 1.0.0
 * @ClassName GmallConfig
 * @Describe
 */
public class GmallConfig {
    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL2022_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:nwh120,nwh121,nwh122:2181";

    //ClickHouse_Url
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://8123:8123/default";

    //ClickHouse_Driver
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
}
