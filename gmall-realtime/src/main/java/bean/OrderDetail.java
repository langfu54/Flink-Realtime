package bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
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
