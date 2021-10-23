package utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import org.apache.flink.api.common.time.Time;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static utils.JDBCUtil.sqlQuery;
import static utils.RedisUtil.getJedis;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class DimUtil {
    public static JSONObject getDimInfo(Connection connection,String tableName,String id) throws SQLException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException {
//        GmallConfig.PHOENIX_DRIVER.getClass();
//        Class.forName(GmallConfig.PHOENIX_DRIVER);
//        connection = DriverManager.getConnection(GmallConfig.PHOENIX_DRIVER);

//        设计RedisKey
        String redisKey = "DIM:"+tableName+id;

        //获取Jedis连接
        Jedis jedis = getJedis();
        String res = jedis.get(redisKey);
        //查Redis 查不到执行 phoenix语句
        if (res != null){
            jedis.expire(redisKey,24*60*60);
            jedis.close();
            return JSON.parseObject(res);
        }else {
            String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id +"'";
            List<JSONObject> list = sqlQuery(connection, sql, JSONObject.class, true);
            //先将查到的数据放到Redis，更新Redis保存时间
            jedis.set(redisKey,list.get(0).toJSONString());
            jedis.expire(redisKey, 24 * 60 * 60);
            //归还连接
            jedis.close();
            return list.get(0);
        }

    }

    //在维度信息更新时，需要删除redis的数据，保证数据的一致性
    public static void deleteRedisDim(String tableName,String id){
        Jedis jedis = getJedis();
        //  设计RedisKey
        String redisKey = "DIM:"+tableName+id;
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        JSONObject dim_user_info = getDimInfo(connection, "DIM_USER_INFO", "1000");
        System.out.println(dim_user_info.toJSONString());
        connection.close();
    }

}
