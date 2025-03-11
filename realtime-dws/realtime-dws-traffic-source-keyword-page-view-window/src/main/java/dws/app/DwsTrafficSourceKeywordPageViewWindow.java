package dws.app;

import dws.function.keywordUDTF;
import dws.util.KeywordUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import rainbow.realtime.common.Base.BaseSQLApp;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.util.FlinkSourceUtil;
import rainbow.realtime.common.util.SQLUtil;

/**
 * @author rainbow
 * @time 2025-03-11 11:55
 * @description ...
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,
                4,
                "dws_traffic_source_keyword_page_view_window"
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 注册自定义函数到表环境中
        tableEnv.createTemporarySystemFunction("ik_analyze", keywordUDTF.class);
        //TODO 从页面日志中读取数据，创建动态表 并制定watermark的生成策略以及提取事件时间字段
        tableEnv.executeSql(
                "create table page_log(                     " +
                        " page map<string, string>,         " +
                        " ts bigint,                        " +
                        " et as to_timestamp_ltz(ts, 3),    " +
                        " watermark for et as et            " +
                        ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE, "dws_traffic_source_keyword_page_view_window")
        );

        //tableEnv.executeSql("select * from page_log").print();

        //TODO 过滤出搜索行为  last_page_id = 'search' and item_type = 'keyword' and item is not null
        Table searchTable = tableEnv.sqlQuery(
                "select " +
                        "   page['item'] fullword, " +
                        "   et " +
                        "from page_log " +
                        "where  page['last_page_id'] = 'search' " +
                        "and    page['item_type'] = 'keyword' " +
                        "and    page['item'] is not null "
        );
        tableEnv.createTemporaryView("search_table", searchTable);

        //TODO 调用自定义函数分词    并和原表的其他字段进行join
        Table splitTable = tableEnv.sqlQuery(
                "SELECT                 " +
                        "   keyword,    " +
                        "   et          " +
                        "FROM search_table,         " +
                        "LATERAL TABLE(ik_analyze(fullword)) t(keyword) "
        );
        tableEnv.createTemporaryView("split_table", splitTable);

        //tableEnv.executeSql("select * from split_table").print();
        //TODO 分组，开窗，聚合
        Table resTable = tableEnv.sqlQuery(
                "SELECT                             \n" +
                        "    date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,            \n" +
                        "    date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,                \n" +
                        "    date_format(window_start, 'yyyy-MM-dd') cur_date,                    \n" +
                        "    keyword,                   \n" +
                        "    count(*) keyword_count     \n" +
                        "FROM                           \n" +
                        "    TABLE (                    \n" +
                        "        TUMBLE (               \n" +
                        "            TABLE split_table, \n" +
                        "            DESCRIPTOR (et),   \n" +
                        "            INTERVAL '20' SECOND  \n" +
                        "        )                      \n" +
                        "    )                          \n" +
                        "GROUP BY                       \n" +
                        "    window_start,              \n" +
                        "    window_end,                \n" +
                        "    keyword"
        );
        //resTable.execute().print();

        //TODO 将聚合的结果写到doris
        //先创建动态表
        tableEnv.executeSql(
                "create table dws_traffic_source_keyword_page_view_window(" +
                        "  stt string, " +  // 2023-07-11 14:14:14
                        "  edt string, " +
                        "  cur_date string, " +
                        "  keyword string, " +
                        "  keyword_count bigint " +
                        ")with(" +
                        " 'connector' = 'doris'," +
                        " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +
                        "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
                        "  'username' = 'root'," +
                        "  'password' = '000000', " +
                        "  'sink.properties.format' = 'json', " +
                        "  'sink.buffer-count' = '4', " +
                        "  'sink.buffer-size' = '4086'," +
                        "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                        "  'sink.properties.read_json_by_line' = 'true' " +
                        ")");

        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");
    }
}
