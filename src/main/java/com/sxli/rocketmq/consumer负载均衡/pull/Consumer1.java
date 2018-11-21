package com.sxli.rocketmq.consumer负载均衡.pull;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueConsistentHash;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.Set;

public class Consumer1 {

    public static void main(String[] args) throws Exception {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("consumer_group");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.registerMessageQueueListener("orders_new", new MessageQueueListener() {
            @Override
            public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
                System.out.println(mqAll);
                System.out.println(mqDivided);
                System.out.println("有新的Consumer加入消费者群组");
            }
        });
        consumer.start();
    }

}
