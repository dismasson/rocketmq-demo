package com.sxli.rocketmq.provider;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * 给消息设置过滤规则 Demo
 */
public class FilterProvider {
    public static void main(String[] args) {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("filter_provider");
            producer.setNamesrvAddr("localhost:9876");
            producer.start();
            // 设置过滤规则是key=value_a
            Message message_a = new Message("orders", "TAG_A", "order.id.0001".getBytes());
            message_a.putUserProperty("key", "value_a");
            // 设置过滤规则是key=value_b
            Message message_b = new Message("orders", "TAG_B", "order.id.0002".getBytes());
            message_a.putUserProperty("key", "value_b");
            // 批量发送
            List<Message> messages = new ArrayList<>(2);
            messages.add(message_a);
            messages.add(message_b);
            SendResult sendResult = producer.send(messages);
            System.out.println(messages);
            System.out.println(sendResult + " send over!");
            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
