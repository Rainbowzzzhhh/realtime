package dws.function;

import dws.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author rainbow
 * @time 2025-03-11 11:47
 * @description ...
 */


@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class keywordUDTF extends TableFunction<Row> {

    public void eval(String text) {
        for (String keyword : KeywordUtil.analyze(text, true)) {
            // use collect(...) to emit a row
            collect(Row.of(keyword));
        }
    }
}
