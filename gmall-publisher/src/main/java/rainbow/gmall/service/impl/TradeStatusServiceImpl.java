package rainbow.gmall.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import rainbow.gmall.bean.TradeProvinceOrderAmount;
import rainbow.gmall.mapper.TradeStatusMapper;
import rainbow.gmall.service.TradeStatusService;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

/**
 * @author rainbow
 * @time 2025-03-15 11:41
 * @description ...
 */

@Service
public class TradeStatusServiceImpl implements TradeStatusService {

    @Autowired
    private TradeStatusMapper tradeStatusMapper;

    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStatusMapper.selectGMV(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getProvinceAmount(Integer date) {
        return tradeStatusMapper.selectProvinceAmount(date);
    }
}
