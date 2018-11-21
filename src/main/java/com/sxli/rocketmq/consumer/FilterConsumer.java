package com.sxli.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 过滤规则消费者 Demo
 */
public class FilterConsumer {

    private static void filterA() {
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("filter_consumer_a");
            consumer.setNamesrvAddr("localhost:9876");
            consumer.subscribe("orders", MessageSelector.bySql("key = value_a"));
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    Message message = msgs.get(0);
                    System.out.println("FILTER_A:" + new String(message.getBody()));
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void filterB() {
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("filter_consumer_b");
            consumer.setNamesrvAddr("localhost:9876");
            consumer.subscribe("orders", MessageSelector.bySql("key = value_b"));
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    Message message = msgs.get(0);
                    System.out.println("FILTER_B:" + new String(message.getBody()));
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        filterA();
        filterB();
    }
}
