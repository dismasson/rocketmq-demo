package com.sxli.rocketmq.配置MessageQueue的个数;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * 配置topic的MessageQueue的个数
 */
public class Provider {
    public static void main(String[] args) {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("message_size_provider");
            producer.setNamesrvAddr("localhost:9876");
            // 设置数量为16个MessageQueue
            producer.setDefaultTopicQueueNums(16);
            producer.start();
            // 总共发送16条消息
            for (int i = 0; i < 16; i++) {
                Message message = new Message("orders", ("order.id." + i).getBytes());
                SendResult sendResult = producer.send(message);
                System.out.println(message);
                System.out.println(sendResult + " send over!");
                Thread.sleep(200);
            }
            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
