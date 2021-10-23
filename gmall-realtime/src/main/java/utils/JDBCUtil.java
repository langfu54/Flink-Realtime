package utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public class JDBCUtil {
    /**
     * a generic function of execute jdbc query
     * select * from table where id = 'id'
     * @param querySql
     * @param clz the class of Generic T
     * @param toCamal convert fileds to CamalCasing Notation
     * @return a list of T obj
     */
//    String sql = "select * from " + querySql + "where id='" + id +"'";
    public static <T> List<T> sqlQuery(Connection connection, String querySql, Class<T> clz, Boolean toCamal) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        //存放结果
        ArrayList<T> list = new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        ResultSet resultSet = preparedStatement.executeQuery();

        //解析resultSer
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()){
            T t = clz.newInstance();
            for (int i = 1; i < columnCount + 1; i++) {  //JDBC 的连接，下标都是1开始
                String columnName = metaData.getColumnName(i);
                if (toCamal) {
                    columnName = CaseFormat.LOWER_UNDERSCORE   //mysql 字段格式
                            .to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());  //bean 格式
                }
                Object value = resultSet.getObject(i);

                BeanUtils.setProperty(t, columnName, value);
            }
            list.add(t);
        }

        preparedStatement.close();
        resultSet.close();

        return list;
    }

//    public static void main(String[] args) throws ClassNotFoundException, SQLException, IllegalAccessException, InvocationTargetException, InstantiationException {
//        Class.forName(GmallConfig.PHOENIX_DRIVER);
//        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
//
//        String sql = "select * from "+ GmallConfig.HBASE_SCHEMA + ".DIM_USER_INFO where id='969'";
//
//        List<JSONObject> resList = sqlQuery(connection, sql, JSONObject.class, true);
//
//        System.out.println(resList.toString());
//
//        connection.close();
//    }

}
