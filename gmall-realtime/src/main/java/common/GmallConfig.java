package common;

public class GmallConfig {
    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop101:2181";

    public static final String CLICKHOUSE_SERVER = "jdbc:clickhouse://hadoop102:8123/default";

    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
}
//create schema GMALL2021_REALTIME;