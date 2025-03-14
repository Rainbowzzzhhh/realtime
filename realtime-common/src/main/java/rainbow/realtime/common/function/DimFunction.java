package rainbow.realtime.common.function;

import com.alibaba.fastjson.JSONObject;

/**
 * @author rainbow
 * @time 2025-03-14 01:22
 * @description ...
 */
public interface DimFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj);

    String getTableName();

    String getRowKey(T obj);
}
