package com.sxli.rocketmq.配置MessageQueue的个数;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * DefaultMQPushConsumer Demo
 */
public class Consumer {

    public static void main(String[] args) {
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("message_size_consumer");
            consumer.setNamesrvAddr("localhost:9876");
            consumer.subscribe("orders", "*");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    Message message = msgs.get(0);
                    System.out.println("consumer_1:" + new String(message.getBody()));
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
