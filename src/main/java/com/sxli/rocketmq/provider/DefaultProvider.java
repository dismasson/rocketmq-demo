package com.sxli.rocketmq.provider;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * DefaultMQProducer Demo
 */
public class DefaultProvider {
    public static void main(String[] args) {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("default_group");
            producer.setNamesrvAddr("localhost:9876");
            producer.start();
            for (int i = 0; i < 100; i++) {
                Message message = new Message("orders", ("order.id." + i).getBytes());
                SendResult sendResult = producer.send(message);
                System.out.println(message);
                System.out.println(sendResult + " send over!");
            }
            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
