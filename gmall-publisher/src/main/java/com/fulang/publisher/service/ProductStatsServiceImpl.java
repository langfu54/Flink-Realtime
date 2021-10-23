package com.fulang.publisher.service;

import com.fulang.publisher.mapper.ProductStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.09
 * @desc:
 */

@Service
public class ProductStatsServiceImpl implements ProductStatsService {
    @Autowired
    ProductStatsMapper productStatsMapper;  //idea 异常，忽略

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }

}
