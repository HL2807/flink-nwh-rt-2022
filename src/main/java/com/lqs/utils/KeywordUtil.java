package com.lqs.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author lqs
 * @Date 2022年03月15日 10:49:22
 * @Version 1.0.0
 * @ClassName KeywordUtil
 * @Describe
 */
public class KeywordUtil {

    public static List<String> splitKeyword(String keyword) throws IOException {

        //创建适合用于存放结果数据
        ArrayList<String> resultList = new ArrayList<>();

        StringReader reader = new StringReader(keyword);

        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        while (true){
            Lexeme next = ikSegmenter.next();

            if (next!=null){
                String word = next.getLexemeText();
                resultList.add(word);
            }else {
                break;
            }
        }

        //返回数据
        return resultList;

    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyword("基于flink的电商实时数仓项目"));
    }

}
