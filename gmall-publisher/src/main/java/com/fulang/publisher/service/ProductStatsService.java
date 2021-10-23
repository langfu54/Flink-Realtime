package com.fulang.publisher.service;

import java.math.BigDecimal;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.09
 * @desc:
 */
public interface ProductStatsService {
        //获取某一天的总交易额
        BigDecimal getGMV(int date);

}
