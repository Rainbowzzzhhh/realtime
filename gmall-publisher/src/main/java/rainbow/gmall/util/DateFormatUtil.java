package rainbow.gmall.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

/**
 * @author rainbow
 * @time 2025-03-15 11:48
 * @description ...
 */
public class DateFormatUtil {
    public static Integer now() {
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
