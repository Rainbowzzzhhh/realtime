package rainbow.gmall.controller;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import rainbow.gmall.bean.TrafficUvCt;
import rainbow.gmall.service.TrafficStatusService;
import rainbow.gmall.util.DateFormatUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author rainbow
 * @time 2025-03-15 15:58
 * @description ...
 */

@RestController
public class TrafficStatusController {

    @Autowired
    private TrafficStatusService trafficStatusService;

    @RequestMapping("/ch")
    public String getChUvCt(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit
    ) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficUvCt> chUvCtList = trafficStatusService.getChUvCt(date, limit);

        List<String> chList = new ArrayList<String>();
        List<Integer> UvCtList = new ArrayList<Integer>();

        for (TrafficUvCt trafficUvCt : chUvCtList) {
            chList.add(trafficUvCt.getCh());
            UvCtList.add(trafficUvCt.getUvCt());
        }

        return "{\"status\": 0,\"data\": {\"categories\": [\""+ StringUtils.join(chList,"\",\"") +"\"]," +
                "\"series\": [{\"name\": \"渠道\",\"data\": ["+StringUtils.join(UvCtList,",")+"]}]}}";
    }

}
