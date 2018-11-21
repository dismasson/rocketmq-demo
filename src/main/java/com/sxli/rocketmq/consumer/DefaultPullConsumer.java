package com.sxli.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DefaultMQPullConsumer Demo
 */
public class DefaultPullConsumer {

    private static final Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        offsetTable.put(mq, offset);
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = offsetTable.get(mq);
        if (offset != null)
            return offset;
        return 0;
    }

    public static void main(String[] args) {
        try {
            DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("default_consumer");
            consumer.setNamesrvAddr("localhost:9876");
            consumer.start();

            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("orders");
            for (MessageQueue mq : mqs) {
                //消息未到达默认是阻塞10秒，private long consumerPullTimeoutMillis = 1000 * 10;
                PullResultExt pullResult = (PullResultExt) consumer.pullBlockIfNotFound(
                        mq, null, getMessageQueueOffset(mq), 32);
                putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        List<MessageExt> messageExtList = pullResult.getMsgFoundList();
                        for (MessageExt m : messageExtList) {
                            System.out.println(new String(m.getBody()));
                        }
                        break;
                    case NO_MATCHED_MSG:
                        break;
                    case NO_NEW_MSG:
                        break;
                    case OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
