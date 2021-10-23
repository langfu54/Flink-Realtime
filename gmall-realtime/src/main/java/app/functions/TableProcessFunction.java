package app.functions;

import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject>{
    private Connection connection;
    private OutputTag<JSONObject> hbaseTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> hbaseTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.hbaseTag = hbaseTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //TODO 1. 读取状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess != null) {  //对于未添加的表，可能为null

        //TODO 2. 过滤数据  .比对建表的字段和原字段，只保留需要的字段

        filerFileds(tableProcess.getSinkColumns(),value.getJSONObject("after"));

        //TODO 3. 分流
            //将输出表/主题信息写入Value
            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            System.out.println(sinkType);
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //Kafka数据,写入主流
                out.collect(value);
                System.out.println(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                //HBase数据,写入侧输出流
                ctx.output(hbaseTag, value);
            }

        } else {
            System.out.println("该组合Key：" + key + "不存在！");
        }

    }

    private void filerFileds(String sinkColumns, JSONObject after) {
        String[] split = sinkColumns.split(",");
        List<String> columns = Arrays.asList(split);

        after.entrySet().removeIf(next -> !columns.contains(next.getKey()));


//
//        Set<Map.Entry<String, Object>> entries = after.entrySet();
//        for (Map.Entry<String, Object> entry : entries) {
//            if (!columns.contains(entry.getKey())){
//                entries.remove(entry);
//            }
//        }
    }


    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //TODO 1.解析数据  String => TableProcess
        TableProcess tableProcess = null;
        String after = JSON.parseObject(value).getString("after");
        if (after != null){  //可能为NULL？  delete已经被过滤
            tableProcess = JSON.parseObject(after, TableProcess.class);
        }

        //TODO 2.检查HBASE表是否存在并建表 create table if not exists xx ()

        if ("hbase".equals(tableProcess.getSinkType())){
            checkorCreateTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()

            );
        }

        //TODO 3.写入状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();    //表名+操作 主键
        broadcastState.put(key, tableProcess);
    }

    private void checkorCreateTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        StringBuffer phoenixSql = new StringBuffer();
        PreparedStatement preparedStatement = null;
        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            phoenixSql.append("CREATE TABLE IF NOT EXISTS " )
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] fields = sinkColumns.split(",");

            for (int i = 0; i < fields.length; i++) {
                phoenixSql.append(fields[i] + " varchar");

                if (sinkPk.equals(fields[i])){
                    phoenixSql.append(" primary key");
                }
                if (i < fields.length - 1 ){
                    phoenixSql.append(",");
                }
            }
            phoenixSql.append(")");
            phoenixSql.append(sinkExtend);

            //构建PS
            preparedStatement = connection.prepareStatement(phoenixSql.toString());
            preparedStatement.execute();
            System.out.println(phoenixSql.toString());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }

        }

    }
}
