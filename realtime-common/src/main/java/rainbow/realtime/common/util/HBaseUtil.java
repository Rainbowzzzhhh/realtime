package rainbow.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import rainbow.realtime.common.constant.Constant;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@Slf4j
public class HBaseUtil {

    // 获取Hbase连接
    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

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
                System.out.println("表空间" + nameSpace + "下的表" + tableName + "已存在");
                return;
            }

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);

            for (String family : families) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes()).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }

            admin.createTable(tableDescriptorBuilder.build());

//            log.info("创建表空间{}下的表{}成功", nameSpace, tableName);
            System.out.println("创建表空间" + nameSpace + "下的表" + tableName + "成功");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 删除HBase表
    public static void dropHBaseTable(Connection hbaseConn, String nameSpace, String tableName) {
        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            // 判断表是否存在
            if (!admin.tableExists(tableNameObj)) {
                log.error("要删除的表空间{}下的表{}不存在", nameSpace, tableName);
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);

//            log.info("删除表空间{}下的表{}成功", nameSpace, tableName);
            System.out.println("删除表空间" + nameSpace + "下的表" + tableName + "成功");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //向表put数据
    public static void putRow(Connection hbaseConn, String nameSpace, String tableName, String rowKey, String family, JSONObject jsonObj) {
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Put put = new Put(rowKey.getBytes());
            Set<String> columns = jsonObj.keySet();
            for (String column : columns) {
                String value = jsonObj.getString(column);
                if (StringUtils.isNotEmpty(value)) {
                    put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
                }
            }
            table.put(put);
            //log.info("表空间{}下的表{}中put数据{}成功", nameSpace, tableName, rowKey);
            System.out.println("表空间" + nameSpace + "下的表" + tableName + "中put数据" + rowKey + "成功");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void delRow(Connection hbaseConn, String nameSpace, String tableName, String rowKey) {
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            table.delete(new Delete(rowKey.getBytes()));
            //log.info("表空间{}下的表{}中delete数据{}成功", nameSpace, tableName, rowKey);
            System.out.println("表空间" + nameSpace + "下的表" + tableName + "中delete数据" + rowKey + "成功");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据rowkey从HBASE表中查询一行数据
     *
     * @param hbaseConn          hbase连接对象
     * @param nameSpace          表空间
     * @param tableName          表名
     * @param rowKey             rowkey
     * @param clz                将查询的一行数据封装为的类型
     * @param isUnderlineToCamel 是否将下划线转换为驼峰命名
     * @param <T>                返回的数据类型为泛型
     * @return 返回的数据类型为泛型
     */
    public static <T> T getRow(Connection hbaseConn, String nameSpace, String tableName, String rowKey, Class<T> clz, boolean... isUnderlineToCamel) {
        boolean defaultIsTTOC = false;

        if (isUnderlineToCamel.length > 0)
            defaultIsTTOC = isUnderlineToCamel[0];

        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);

        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Result result = table.get(new Get(Bytes.toBytes(rowKey)));
            List<Cell> cells = result.listCells();
            if (cells != null && !cells.isEmpty()) {
                //定义一个对象，用于封装查询出来的一行数据
                T obj = clz.newInstance();

                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if (defaultIsTTOC) {
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                return obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

//--------------------------------------------------Async---------------------------------------------------------------------------------------------

    /**
     * 获取到 Hbase 的异步连接
     *
     * @return 得到异步连接对象
     */
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭 hbase 异步连接
     *
     * @param asyncConn 异步连接
     */
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null && !asyncConn.isClosed()) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 异步的从 hbase 读取维度数据
     *
     * @param hBaseAsyncConn hbase 的异步连接
     * @param nameSpace      命名空间
     * @param tableName      表名
     * @param rowKey         rowKey
     * @return 读取到的维度数据, 封装到 json 对象中.
     */
    public static JSONObject readDimAsync(AsyncConnection hBaseAsyncConn, String nameSpace, String tableName, String rowKey) {
        AsyncTable<AdvancedScanResultConsumer> asyncTable
                = hBaseAsyncConn.getTable(TableName.valueOf(nameSpace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            // 获取 result
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
            if (cells != null && !cells.isEmpty()) {
                JSONObject dim = new JSONObject();

                for (Cell cell : cells) {
                    // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    dim.put(columnName, columnValue);
                }
                return dim;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }


    public static void main(String[] args) throws IOException {
        Connection conn = getHBaseConnection();
        JSONObject jsonObj = getRow(conn, Constant.HBASE_NAMESPACE, "dim_base_trademark", "1", JSONObject.class);
        System.out.println(jsonObj);
        closeHBaseConn(conn);
    }
}































