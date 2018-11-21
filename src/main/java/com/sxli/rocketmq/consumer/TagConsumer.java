package com.sxli.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 标签消息消费者 Demo
 */
public class TagConsumer {

    private static void tagA() {
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag_consumer_a");
            consumer.setNamesrvAddr("localhost:9876");
            consumer.subscribe("orders", "TAG_A");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    Message message = msgs.get(0);
                    System.out.println("TAG_A:" + new String(message.getBody()));
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void tagB() {
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag_consumer_b");
            consumer.setNamesrvAddr("localhost:9876");
            consumer.subscribe("orders", "TAG_B");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    Message message = msgs.get(0);
                    System.out.println("TAG_B:" + new String(message.getBody()));
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        tagA();
        tagB();
    }
}
