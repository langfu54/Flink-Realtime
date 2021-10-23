package com.fulang.publisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.09
 * @desc:
 */
public interface ProductStatsMapper {
    //获取商品交易额
    @Select("select sum(order_amount) order_amount  " +
            "from product_stats_2021 where toYYYYMMDD(stt)=#{date}")
    BigDecimal getGMV(int date);

}
