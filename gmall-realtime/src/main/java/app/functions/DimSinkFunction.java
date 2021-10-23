package app.functions;

import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

import static utils.DimUtil.deleteRedisDim;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);    //hbase 为保证批次提交，自动提交设置为FALSE，需要修改
    }

    //value:{"sinkTable":"dim_base_trademark","database":"gmall-210325-flink","before":{"tm_name":"atguigu","id":12},"after":{"tm_name":"Atguigu","id":12},"type":"update","tableName":"base_trademark"}
    //SQL：upsert into db.tn(id,tm_name) values('...','...')
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;
        try {
            //获取SQL语句
            String upsertSql = genUpsertSql(value.getString("sinkTable"),
                    value.getJSONObject("after"));
            System.out.println(upsertSql);

            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);


            //如果为DIM 的更新操作，删除redis结果，保持数据一致性
            String table = value.getString("sinkTable");
            String id = value.getJSONObject("after").getString("id");
            if ("update".equals(value.getString("type"))){
                deleteRedisDim(table.toUpperCase(),id);
            }

            //执行插入操作
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    //data:{"tm_name":"Atguigu","id":12}
    //SQL：upsert into db.tn(id,tm_name,aa,bb) values('...','...','...','...')
    private String genUpsertSql(String sinkTable, JSONObject data) {

        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();

        //keySet.mkString(",");  =>  "id,tm_name"  == join
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(keySet, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";
    }
}
