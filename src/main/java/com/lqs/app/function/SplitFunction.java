package com.lqs.app.function;

import com.lqs.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @Author lqs
 * @Date 2022年03月15日 16:45:21
 * @Version 1.0.0
 * @ClassName SplitFunction
 * @Describe
 */

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str){
        try {
            //分词
            List<String> words = KeywordUtil.splitKeyword(str);

            //遍历并写出
            for (String word : words) {
                collect(Row.of(word));
            }
        }catch (IOException e){
            collect(Row.of(str));
        }
    }

}
