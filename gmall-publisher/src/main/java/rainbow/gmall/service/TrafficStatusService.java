package rainbow.gmall.service;

import rainbow.gmall.bean.TrafficUvCt;

import java.util.List;

/**
 * @author rainbow
 * @time 2025-03-15 15:54
 * @description ...
 */
public interface TrafficStatusService {
    //获取某天各个渠道独立访客
    List<TrafficUvCt> getChUvCt(Integer date,Integer limit);
}
