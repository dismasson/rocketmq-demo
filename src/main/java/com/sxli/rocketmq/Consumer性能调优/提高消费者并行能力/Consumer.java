package com.sxli.rocketmq.Consumer性能调优.提高消费者并行能力;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueConsistentHash;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class Consumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_group", null,
                new AllocateMessageQueueConsistentHash());
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.subscribe("orders_new", (String) null);
        // 设置线程池数量
        consumer.setConsumeThreadMin(50);
        consumer.setConsumeThreadMax(100);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println("ThreadName:" + Thread.currentThread().getName() + "\tMessageQueueId:"
                            + msg.getQueueId() + "\tMessageBody:" + new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }
}
