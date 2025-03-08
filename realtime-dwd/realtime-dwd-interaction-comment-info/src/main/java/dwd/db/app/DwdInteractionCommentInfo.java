package dwd.db.app;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import rainbow.realtime.common.Base.BaseSQLApp;
import rainbow.realtime.common.constant.Constant;
import rainbow.realtime.common.util.SQLUtil;

/**
 * @author rainbow
 * @time 2025-03-06 21:44
 * @description 用表名当消费者组名和checkpoint的文件路径
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        DwdInteractionCommentInfo dwdInteractionCommentInfo = new DwdInteractionCommentInfo();
        dwdInteractionCommentInfo.start(10012, 4, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 3.从kafka的topic_db主题中读取数据 创建动态表   --kafka连接器
        readOdsDb(tableEnv, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        //TODO 4.过滤出评论数据                            --where table = 'comment_info' type = insert
        Table commentInfo = tableEnv.sqlQuery(
                "   select  " +
                        "       `data`['id']    id,                     " +
                        "       `data`['user_id']   user_id,            " +
                        "       `data`['sku_id']    sku_id,             " +
                        "       `data`['appraise']  appraise,           " +
                        "       `data`['comment_txt']   comment_txt,    " +
                        "        ts,                                    " +
                        "        pt                                     " +
                        "    from topic_db                              " +
                        "    where `database`='gmall'                   " +
                        "    and `table`='comment_info'                 " +
                        "    and `type`='insert'                        "
        );

        //将表对象注册进表执行环境中
        tableEnv.createTemporaryView("comment_info", commentInfo);

        //TODO 5.从Hbase中读取字典数据，创建动态表            --hbase连接器
        readBaseDic(tableEnv);

        //TODO 6.将评论表和字典表进行关联                    --lookupJoin
        Table joinedTable = tableEnv.sqlQuery(
                "SELECT \n" +
                        "    c.id,\n" +
                        "    c.user_id,\n" +
                        "    c.sku_id,\n" +
                        "    c.appraise,\n" +
                        "    dic.dic_name appraise_name,\n" +
                        "    c.comment_txt,\n" +
                        "    c.ts\n" +
                        "FROM comment_info AS c\n" +
                        "JOIN base_dic FOR SYSTEM_TIME AS OF c.pt AS dic\n" +
                        "ON c.appraise = dic.dic_code"
        );

        //TODO 7.将关联的结果写到kafka主题中                 --upsertKafka连接器
        //7.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql(
                "CREATE TABLE " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + " (\n" +
                        "    id string,                     \n" +
                        "    user_id string,                \n" +
                        "    sku_id string,                 \n" +
                        "    appraise string,               \n" +
                        "    appraise_name string,          \n" +
                        "    comment_txt string,            \n" +
                        "    ts bigint,                     \n" +
                        "    PRIMARY KEY (id) NOT ENFORCED  \n" +
                        ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO)
        );

        //7.2 写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}
