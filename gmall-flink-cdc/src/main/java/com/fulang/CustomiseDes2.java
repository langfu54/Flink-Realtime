package com.fulang;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:
 *  {
 *         原始数据格式
 *         SourceRecord{sourcePartition={server=mysql_binlog_source},
 *         sourceOffset={file=mysql-bin.000034, pos=1046}}
 *         ConnectRecord{topic='mysql_binlog_source.rt-gmall2021.base_trademark',
 *         kafkaPartition=null, key=Struct{id=11},
 *         keySchema=Schema{mysql_binlog_source.rt_gmall2021.base_trademark.Key:STRUCT},
 *         value=Struct{after=Struct{id=11,tm_name=香奈儿,logo_url=/static/default.jpg},source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=last,db=rt-gmall2021,table=base_trademark,server_id=0,file=mysql-bin.000034,pos=1046,row=0},
 *         op=c,ts_ms=1629715684521},
 *         valueSchema=Schema{mysql_binlog_source.rt_gmall2021.base_trademark.Envelope:STRUCT},
 *         timestamp=null, headers=ConnectHeaders(headers=)}
 *  *       * 封装的数据格式
 *  *      * {
 *  *      * "database":"",
 *  *      * "tableName":"",
 *  *      * "before":{"id":"","tm_name":""....},
 *  *      * "after":{"id":"","tm_name":""....},
 *  *      * "type":"c u d",
 *  *      * //"ts":156456135615
 *  *     }
 */
public class CustomiseDes2 implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject jsonObj = new JSONObject();
        //1.获取db名
        String dbName = sourceRecord.topic();
        //2.获取table名
        Struct struct = (Struct) sourceRecord.value();
        Struct op = struct.getStruct("op");
        //3.获取before数据

        //4.获取after数据

        //5.获取Type

        //6.将数据封装到JSON
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
