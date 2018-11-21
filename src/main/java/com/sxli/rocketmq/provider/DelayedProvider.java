package com.sxli.rocketmq.provider;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * 延时发送消息 Demo
 */
public class DelayedProvider {
    public static void main(String[] args) {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("default_group");
            producer.setNamesrvAddr("localhost:9876");
            producer.start();
            Message message = new Message("orders", "order.id.0001".getBytes());
            // 延时5秒发送
            message.setDelayTimeLevel(5);
            SendResult sendResult = producer.send(message);
            System.out.println(message);
            System.out.println(sendResult + " send over!");
            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
