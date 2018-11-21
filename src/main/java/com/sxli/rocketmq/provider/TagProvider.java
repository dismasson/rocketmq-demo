package com.sxli.rocketmq.provider;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * 给消息贴上标签 Demo
 */
public class TagProvider {
    public static void main(String[] args) {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("tag_provider");
            producer.setNamesrvAddr("localhost:9876");
            producer.start();
            // 贴了标签A的消息
            Message message_a = new Message("orders", "TAG_A", "order.id.0001".getBytes());
            // 贴了标签B的消息
            Message message_b = new Message("orders", "TAG_B", "order.id.0002".getBytes());
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
