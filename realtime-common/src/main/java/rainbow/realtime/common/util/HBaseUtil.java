package rainbow.realtime.common.util;

import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Set;

@Slf4j
public class HBaseUtil {

    // 获取Hbase连接
    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        return ConnectionFactory.createConnection(conf);
    }

    // 关闭Hbase连接
    public static void closeHBaseConn(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    // 创建HBase表
    public static void createHBaseTable(Connection hbaseConn, String nameSpace, String tableName, String... families) {

        if (families.length < 1) {
            log.error("创建HBase表失败，请输入至少一个列族");
            return;
        }

        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            if (admin.tableExists(tableNameObj)) {
                log.warn("表空间{}下的表{}已存在", nameSpace, tableName);
                return;
            }

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);

            for (String family : families) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes()).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }

            admin.createTable(tableDescriptorBuilder.build());

            log.info("创建表空间{}下的表{}成功", nameSpace, tableName);
            //System.out.println("创建表空间" + nameSpace + "下的表" + tableName + "成功");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 删除HBase表
    public static void dropHBaseTable(Connection hbaseConn, String nameSpace, String tableName) {
        try (Admin admin = hbaseConn.getAdmin();) {
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            // 判断表是否存在
            if (!admin.tableExists(tableNameObj)) {
                log.error("要删除的表空间{}下的表{}不存在", nameSpace, tableName);
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);

            log.info("删除表空间{}下的表{}成功", nameSpace, tableName);
            //System.out.println("删除表空间" + nameSpace + "下的表" + tableName + "成功");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //向表put数据
    public static void putRow(Connection hbaseConn, String nameSpace, String tableName, String rowKey, String family, JSONObject jsonObj) {
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj);) {
            Put put = new Put(rowKey.getBytes());
            Set<String> columns = jsonObj.keySet();
            for (String column : columns) {
                String value = jsonObj.getAsString(column);
                if(StringUtils.isNotEmpty(value)){
                    put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
                }
            }
            table.put(put);
            log.info("表空间{}下的表{}中put数据成功", nameSpace, tableName);
            //System.out.println("表空间" + nameSpace + "下的表" + tableName + "put数据成功");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void delRow(Connection hbaseConn, String nameSpace, String tableName, String rowKey){
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj);) {
            table.delete(new Delete(rowKey.getBytes()));
            log.info("表空间{}下的表{}中delete数据成功", nameSpace, tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
