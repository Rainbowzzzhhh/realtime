package rainbow.gmall.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import rainbow.gmall.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author rainbow
 * @time 2025-03-15 11:30
 * @description ...
 */

@Mapper
public interface TradeStatusMapper {
    //获取某天总交易额
    @Select("SELECT sum(order_amount) order_amount FROM dws_trade_province_order_window partition par#{date}")
    BigDecimal selectGMV(Integer date);

    //获取各省份交易额
    @Select("SELECT province_name,sum(order_amount) order_amount FROM dws_trade_province_order_window partition par#{date} group by province_name")
    List<TradeProvinceOrderAmount> selectProvinceAmount(Integer date);

    //

}
