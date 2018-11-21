package com.sxli.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * DefaultMQPushConsumer Demo
 */
public class DefaultPushConsumer {

    /**
     * 有序的消费者
     */
    private static void orderly() {
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("default_consumer");
            consumer.setNamesrvAddr("localhost:9876");
            consumer.subscribe("orders", "*");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    Message message = msgs.get(0);
                    System.out.println(new String(message.getBody()));
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 无序的消费者
     */
    private static void concurrently() {
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("default_consumer");
            consumer.setNamesrvAddr("localhost:9876");
            consumer.subscribe("orders", "*");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    Message message = msgs.get(0);
                    System.out.println(new String(message.getBody()));
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //  有序的消费者
        //orderly();
        //   无顺序的消费者 消费效率要高一些
        concurrently();
    }
}
