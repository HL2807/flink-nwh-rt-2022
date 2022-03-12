package com.lqs.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @Author lqs
 * @Date 2022年03月12日 22:21:38
 * @Version 1.0.0
 * @ClassName DateTimeUtil
 * @Describe 时间戳格式类
 */
public class DateTimeUtil {

   private final static DateTimeFormatter formatter= DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

   public static String toYMDhms(Date date){
       //年、月、日、时、分、秒
       LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
       return formatter.format(localDateTime);
   }

   public static Long toTs(String YmDHms){
       LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formatter);
       return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
   }

}

