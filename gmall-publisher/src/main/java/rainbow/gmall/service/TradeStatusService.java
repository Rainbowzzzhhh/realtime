package rainbow.gmall.service;

import rainbow.gmall.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author rainbow
 * @time 2025-03-15 11:40
 * @description ...
 */
public interface TradeStatusService {
    //获取某天总交易额
    BigDecimal getGMV(Integer date);

    //获取各省份交易额
    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}
