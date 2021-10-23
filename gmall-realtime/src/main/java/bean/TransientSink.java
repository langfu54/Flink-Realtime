package bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * @Author:Langfu54@gmail.com
 * @Date:2021.08
 * @desc:  标记Bean字段，忽略写入到ClickHouse
 */
@Target(ElementType.FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
