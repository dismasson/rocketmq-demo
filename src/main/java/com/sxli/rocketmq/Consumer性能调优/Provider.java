package com.sxli.rocketmq.Consumer性能调优;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class Provider {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("provider_group");
        producer.setNamesrvAddr("localhost:9876");
        producer.setDefaultTopicQueueNums(16);
        producer.start();
        for (int i = 0; i < 160; i++) {
            Message message = new Message("orders_new", ("order.id." + i).getBytes());
            SendResult result = producer.send(message);
            System.out.println(result);
        }
        producer.shutdown();
    }

}
