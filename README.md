

## Flink自定义Source使用泛型有关函数返回值问题

```java
public class KafkaSource<OUT> extends RichSourceFunction<OUT> {
    //巴拉巴拉省略一堆代码...
}
```

如果是这样子的使用泛型的话，而且没有显式地指明返回值的类型的话，肯定会出错。

类似这样子的错：

有两种解决办法：

1. 手动指明返回的数据类型。
2. 实现ResultTypeQueryable接口。

当然这种错误只会在Source的时候出现，在Sink的时候不会出现这样子的错。

为什么呢？

因为在Source的时候，是将数据反序列化的，在Sink的时候是将数据直接序列化的。

```
Exception in thread "main" org.apache.flink.api.common.functions.InvalidTypesException: The return type of function 'Custom Source' could not be determined automatically, due to type erasure. You can give type information hints by using the returns(...) method on the result of the transformation call, or by letting your function implement the 'ResultTypeQueryable' interface.
	at org.apache.flink.api.dag.Transformation.getOutputType(Transformation.java:417)
	at org.apache.flink.streaming.api.datastream.DataStream.getType(DataStream.java:175)
	at org.apache.flink.streaming.api.datastream.DataStream.flatMap(DataStream.java:612)
	at com.lzhpo.flinkkafka.FlinkKafkaConsumerTest.main(FlinkKafkaConsumerTest.java:44)
Caused by: org.apache.flink.api.common.functions.InvalidTypesException: Type of TypeVariable 'OUT' in 'class org.apache.flink.streaming.api.functions.source.RichSourceFunction' could not be determined. This is most likely a type erasure problem. The type extraction currently supports types with generic variables only in cases where all variables in the return type can be deduced from the input type(s). Otherwise the type has to be specified explicitly using type information.
	at org.apache.flink.api.java.typeutils.TypeExtractor.createTypeInfoWithTypeHierarchy(TypeExtractor.java:882)
	at org.apache.flink.api.java.typeutils.TypeExtractor.privateCreateTypeInfo(TypeExtractor.java:803)
	at org.apache.flink.api.java.typeutils.TypeExtractor.createTypeInfo(TypeExtractor.java:769)
	at org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.addSource(StreamExecutionEnvironment.java:1461)
	at org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.addSource(StreamExecutionEnvironment.java:1416)
	at org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.addSource(StreamExecutionEnvironment.java:1398)
	at com.lzhpo.flinkkafka.FlinkKafkaConsumerTest.main(FlinkKafkaConsumerTest.java:31)
```

### 方法1 - 手动指定`returns`返回的类型

`.returns(String.class)`

```java
SingleOutputStreamOperator<String> flatMapStream = consumerStreamSource
    .returns(String.class)
    .flatMap(new FlatMapFunction<String, String>() {
        @Override
        public void flatMap(String value, Collector<String> collector) throws Exception {
            System.out.println("value:" + value);
            collector.collect(value);
        }
    });
```

### 方法2 - 实现`ResultTypeQueryable`接口【推荐】

推荐使用这种，手动指定`returns`返回的类型不推荐。

实现`org.apache.flink.api.java.typeutils.ResultTypeQueryable#getProducedType`这个方法。

`ResultTypeQueryable`源代码：

实现这个接口的`getProducedType`方法就是获取此函数或输入格式产生的数据类型。

```java
package org.apache.flink.api.java.typeutils;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * This interface can be implemented by functions and input formats to tell the framework
 * about their produced data type. This method acts as an alternative to the reflection analysis
 * that is otherwise performed and is useful in situations where the produced data type may vary
 * depending on parametrization.
 *
 */
@Public
public interface ResultTypeQueryable<T> {

	/**
	 * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
	 * 
	 * @return The data type produced by this function or input format.
	 */
	TypeInformation<T> getProducedType();
}
```

来看具体操作：

比如我这里是实现`RichSourceFunction`的话我就需要反序列化，`schema`就是

```java
// schema通过构造函数传入，在`getProducedType`和实现`RichSourceFunction`的run方法中需要用到。
private DeserializationSchema<OUT> schema;
```

```java
/**
 * 获取反序列化的值的数据类型
 *
 * @return The data type produced by this function or input format.
 */
@Override
public TypeInformation<OUT> getProducedType() {
    return schema.getProducedType();
}
```

