package com.lqs.app.function;

import com.alibaba.fastjson.JSONObject;
import com.lqs.common.GmallConfig;
import com.lqs.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @Author lqs
 * @Date 2022年03月09日 21:14:02
 * @Version 1.0.0
 * @ClassName DimSinkFunction
 * @Describe dim数据落盘hbase函数
 *
 * RichSinkFunction：生命周期方法
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    //value:{"sinkTable":"dim_base_trademark","database":"gmall2022","before":{"tm_name":"lqs","id":12},"after":{"tm_name":"lqs","id":12},"type":"update","tableName":"base_trademark"}
    //SQL：upsert into db.tn(id,tm_name) values('...','...')

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement=null;
        try {
            //获取SQL语句
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = genUpsertSql(sinkTable, after);
            System.out.println(upsertSql);

            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            //判断如果当前数据为更新操作，则先删除Redis中的数据
            if ("update".equals(value.getString("type"))){
                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(),after.getString("id"));
            }

            //执行插入操作
            preparedStatement.executeUpdate();
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            if (preparedStatement!=null){
                preparedStatement.close();
            }
        }
        //PreparedStatement preparedStatement=null;
        //try {
        //    //获取SQL语句
        //    String upsertSql = genUpsertSql(value.getString("sinkTable"), value.getJSONObject("after"));
        //    System.out.println(upsertSql);
        //
        //    //预编译SQL
        //    preparedStatement = connection.prepareStatement(upsertSql);
        //
        //    //执行插入操作
        //    preparedStatement.executeUpdate();
        //} catch (SQLException e) {
        //    e.printStackTrace();
        //}finally {
        //    if (preparedStatement!=null){
        //        preparedStatement.close();
        //    }
        //}
    }

    //data:{"tm_name":"lqs","id":12}
    //SQL：upsert into db.tn(id,tm_name,aa,bb) values('...','...','...','...')
    private String genUpsertSql(String sinkTable, JSONObject data) {
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();

        //keySet.mkString(",");  =>  "id,tm_name" <==> StringUtils.join(values,"','")
        return "upsert into "+ GmallConfig.HBASE_SCHEMA+"."+sinkTable+"("+ StringUtils.join(keySet,",")
                +") values('" + StringUtils.join(values,"','")+"')";
    }
}
