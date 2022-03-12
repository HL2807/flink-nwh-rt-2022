package com.lqs.utils;

import com.alibaba.fastjson.JSONObject;
import com.lqs.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * @Author lqs
 * @Date 2022年03月09日 21:36:56
 * @Version 1.0.0
 * @ClassName DimUtil
 * @Describe 查询维度的工具类，使用了
 */
public class DimUtil {

    /**
     * 查询维度表的函数
     * @param connection 链接
     * @param tableName 需要查询的表名
     * @param id 要查询的id
     * @return JSONObject对象
     * @throws Exception
     */
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {
        //查询phoenix之前先查询Redis
        Jedis jedis = RedisUtil.getJedis();
        //
        //DIM:DIM_USER_INFO:1311
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoJsonSte = jedis.get(redisKey);
        if (dimInfoJsonSte != null) {
            //重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            //归还连接
            jedis.close();
            //返回结果
            return JSONObject.parseObject(dimInfoJsonSte);
        }

        //拼接查询语句
        //select * from db.tn where id='18';
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";

        //查询phoenix
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfoJson = queryList.get(0);

        //在返回结果之前，将数据写入Redis
        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        //return queryList.get(0);

        //返回结果
        return dimInfoJson;
    }

    /**
     * 删除要更新数据的老数据缓存
     * @param tableName 表名
     * @param id 要删除的id（要更新的id）
     */
    public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //long start = System.currentTimeMillis();

        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "15"));

        //long end = System.currentTimeMillis();
        //System.out.println(getDimInfo(connection, "DIM_USER_INFO", "1311"));
        //long end2 = System.currentTimeMillis();
        //System.out.println(getDimInfo(connection, "DIM_USER_INFO", "974"));
        //long end3 = System.currentTimeMillis();
        //
        //System.out.println(end - start);
        //System.out.println(end2 - end);
        //System.out.println(end3 - end2);

        connection.close();
    }

}
