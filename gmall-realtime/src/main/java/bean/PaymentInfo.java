package bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
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
