package app.functions;


import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import utils.DimUtil;
import utils.ThreadPoolUtil;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimAsyncFunctionI<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                //获取ID
                String id = getKey(input);

                //查询维度信息
                try {
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
                    if (dimInfo != null){
                        //补充维度信息
                        join(input,dimInfo);
                    }

                } catch (Exception e ){
                    e.printStackTrace();
                }

                //输出结果
                resultFuture.complete(Collections.singletonList(input));
            }
        });

    }


    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("timeout" + input);
    }

    @Override
    public void close() throws Exception {
        if(connection != null){
            connection.close();
        }
    }
}
