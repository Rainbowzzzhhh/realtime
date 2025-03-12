package rainbow.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author rainbow
 * @time 2025-03-12 19:07
 * @description ...
 */
public class BeanToJsonStrMapFunction<T> implements MapFunction<T, String> {

    @Override
    public String map(T value) {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(value, config);
    }
}
