package com.fxh.kafka.base;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Author FanXH
 * @Date 2023/6/29 14:47
 * @Description
 */
public class MyConsumer {
    private static final String BOOTSTRAP_SERVERS ="10.211.55.12:9092,10.211.55.13:9092,10.211.55.14:9092";
    private static final String TOPIC = "disTopic";
    public static void main(String[] args) {
//PART1:设置发送者相关属性
        Properties props = new Properties();
//kafka地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        //每个消费者要指定一个group
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "disTopicGroup");
        //key序列化类
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //value序列化类
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC));
        while (true) {
            //PART2:拉取消息
            // 100毫秒超时时间
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofNanos(100));

            //PART3:处理消息
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("offset = " + record.offset() + ";key = " + record.key() + "; value= " + record.value());
            }
            //提交offset，消息就不会重复推送。
            consumer.commitSync();
            //同步提交，表示必须等到offset提交完毕，再去消费下一批数据。
            // consumer.commitAsync();
            //异步提交，表示发送完提交offset请求后，就开始消费下一 批数据了。不用等到Broker的确认。
        } }
}
