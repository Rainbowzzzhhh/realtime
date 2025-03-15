package rainbow.gmall.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import rainbow.gmall.bean.TradeProvinceOrderAmount;
import rainbow.gmall.service.TradeStatusService;
import rainbow.gmall.service.impl.TradeStatusServiceImpl;
import rainbow.gmall.util.DateFormatUtil;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author rainbow
 * @time 2025-03-15 11:45
 * @description ...
 */

@RestController
public class TradeStatusController {

    @Autowired
    private TradeStatusService tradeStatusService;

    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            //说明请求没有传递日期参数，将当天日期作为查询的日期
            date = DateFormatUtil.now();
        }
        BigDecimal gmv = tradeStatusService.getGMV(date);
        return "{\"status\":0,\"data\":" + gmv + "}";
    }

    @RequestMapping("/province")
    public String getProvinceAmount(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            //说明请求没有传递日期参数，将当天日期作为查询的日期
            date = DateFormatUtil.now();
        }
        //查询
        List<TradeProvinceOrderAmount> provinceAmountList = tradeStatusService.getProvinceAmount(date);
        //封装为json
        StringBuilder sb = new StringBuilder();
        sb.append("{\"status\": 0,\"msg\": \"\",\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceAmountList.size(); i++) {
            TradeProvinceOrderAmount tradeProvinceOrderAmount = provinceAmountList.get(i);
            sb.append("{\"name\":\"" + tradeProvinceOrderAmount.getProvinceName() + "\",\"value\":" + tradeProvinceOrderAmount.getOrderAmount() + "}");
            if (i < provinceAmountList.size() - 1)
                sb.append(",");
        }
        sb.append("],\"valueName\":\"交易额\"}}");
        return sb.toString();
    }
}
