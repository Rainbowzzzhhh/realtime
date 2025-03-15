package rainbow.gmall.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import rainbow.gmall.bean.TrafficUvCt;
import rainbow.gmall.mapper.TrafficStatusMapper;
import rainbow.gmall.service.TrafficStatusService;

import java.util.List;

/**
 * @author rainbow
 * @time 2025-03-15 15:55
 * @description ...
 */

@Service
public class TrafficStatusServiceImpl implements TrafficStatusService {

    @Autowired
    private TrafficStatusMapper trafficStatusMapper;

    @Override
    public List<TrafficUvCt> getChUvCt(Integer date, Integer limit) {
        return trafficStatusMapper.selectChUvCt(date, limit);
    }
}
