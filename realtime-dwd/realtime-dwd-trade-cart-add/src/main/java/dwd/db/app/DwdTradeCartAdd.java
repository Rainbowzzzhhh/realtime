package dwd.db.app;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import rainbow.realtime.common.Base.BaseSQLApp;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.util.SQLUtil;

/**
 * @author rainbow
 * @time 2025-03-07 20:37
 * @description ...
 */
public class DwdTradeCartAdd extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeCartAdd().start(
                10013, 4, Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 从kafka的topic_db读取数据，创建动态表
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);

        //TODO 过滤出加购数据database = 'gmall',table = 'cate_info',type = 'insert' or 'update' 修改的一定是加购商品的数量
        Table cartInfo = tableEnv.sqlQuery(
                " SELECT\n" +
                        "    `data`['id'] id,\n" +
                        "    `data`['user_id'] user_id,\n" +
                        "    `data`['sku_id'] sku_id,\n" +
                        "    if(type = 'insert', `data`['sku_num'], cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num,\n" +
                        "    ts \n" +
                        " FROM topic_db \n" +
                        " where `database` = 'gmall'\n" +
                        " and `table` = 'cart_info'\n" +
                        " and (\n" +
                        "    `type` = 'insert' \n" +
                        "    or (\n" +
                        "        `type` = 'update' \n" +
                        "        and `old`['sku_num'] is not null\n" +
                        "        and cast(`old`['sku_num'] as int) < cast(`data`['sku_num'] as int)\n" +
                        "        )\n" +
                        "    )");
        //cartInfo.execute().print();
        tableEnv.createTemporaryView("cart_info", cartInfo);

        //TODO 将过滤的数据写入kafka主题中
        //创建动态表
        tableEnv.executeSql(
                "CREATE TABLE " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(\n" +
                        "    id string,             \n" +
                        "    user_id string,        \n" +
                        "    sku_id string,         \n" +
                        "    sku_num string,        \n" +
                        "    ts bigint,             \n" +
                        "    PRIMARY KEY (id) NOT ENFORCED  \n" +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
        //写入
        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
