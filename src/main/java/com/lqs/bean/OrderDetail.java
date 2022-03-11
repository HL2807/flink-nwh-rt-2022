package com.lqs.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @Author lqs
 * @Date 2022年03月11日 17:26:13
 * @Version 1.0.0
 * @ClassName OrderDetail
 * @Describe
 */
@Data
public class OrderDetail {
    Long id;
    Long order_id;
    Long sku_id;
    BigDecimal order_price;
    Long sku_num;
    String sku_name;
    String create_time;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;
}
