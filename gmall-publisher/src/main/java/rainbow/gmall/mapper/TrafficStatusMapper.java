package rainbow.gmall.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import rainbow.gmall.bean.TrafficUvCt;

import java.util.List;

/**
 * @author rainbow
 * @time 2025-03-15 15:43
 * @description ...
 * 流量域统计
 */

@Mapper
public interface TrafficStatusMapper {
    //获取某天各个渠道独立访客
    @Select("SELECT ch,sum(uv_ct) uv_ct from dws_traffic_vc_ch_ar_is_new_page_view_window partition par#{date} group by ch order by uv_ct desc LIMIT #{limit}")
    List<TrafficUvCt> selectChUvCt(@Param("date") Integer date, @Param("limit") Integer limit);
}
