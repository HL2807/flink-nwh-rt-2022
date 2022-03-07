package com.lqs.app.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @Author lqs
 * @Date 2022年03月04日 11:46:02
 * @Version 1.0.0
 * @ClassName CustomerDeserialization
 * @Describe 序列化自定义
 */
public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {

    /**
     * 封装的数据格式
     * {
     * "database":"",
     * "tableName":"",
     * "before":{"id":"","tm_name":""....},
     * "after":{"id":"","tm_name":""....},
     * "type":"c u d",
     * //"ts":156456135615
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //创建JSON对象用于存储最终的数据
        JSONObject result = new JSONObject();

        //获取库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        Struct value = (Struct) sourceRecord.value();

        //获取"before"数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before!=null){
            Schema beforeSchem = before.schema();
            List<Field> beforeFields = beforeSchem.fields();
            for (Field beforeField : beforeFields) {
                Object beforeValue = before.get(beforeField);
                beforeJson.put(beforeField.name(),beforeValue);
            }
        }

        //获取“after"数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after!=null){
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for (Field field : afterFields) {
                Object afterValue = after.get(field);
                afterJson.put(field.name(),afterValue);
            }
        }

        //获取操作类型 CREATE UPDATE DELETE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)){
            type="insert";
        }

        //将字段写入JSON对象
        result.put("database",database);
        result.put("tableName",tableName);
        result.put("before",beforeJson);
        result.put("after",afterJson);
        result.put("type",type);

        //输出数据
        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
