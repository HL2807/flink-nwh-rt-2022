package com.lqs.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @Author lqs
 * @Date 2022年03月12日 21:07:35
 * @Version 1.0.0
 * @ClassName PaymentInfo
 * @Describe 支付实体类
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}
