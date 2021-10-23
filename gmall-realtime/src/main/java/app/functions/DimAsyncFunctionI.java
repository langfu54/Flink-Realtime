package app.functions;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 */
public interface DimAsyncFunctionI<T> {
    //    //抽象方法，在外部传入T的信息，获取ID字段
    //    public abstract void join(JSONObject dimInfo, T input);
    //    public abstract String getKey(T input);
    //抽象方法，在外部传入T的信息，获取ID字段
    void join(T input,JSONObject dimInfo ) throws ParseException;

    String getKey(T input);
}
