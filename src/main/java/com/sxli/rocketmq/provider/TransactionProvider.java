package com.sxli.rocketmq.provider;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 使用事务确保消息一致性 Demo
 */
public class TransactionProvider {

    private static AtomicInteger transactionIndex = new AtomicInteger(0);

    private static ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        try {
            TransactionMQProducer producer = new TransactionMQProducer("transaction_provider");
            producer.setNamesrvAddr("localhost:9876");
            // 设置线程池
            producer.setExecutorService(Executors.newFixedThreadPool(8));
            // 设置事务监听器
            producer.setTransactionListener(new TransactionListener() {
                @Override
                public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                    int value = transactionIndex.getAndIncrement();
                    int status = value % 3;
                    localTrans.put(msg.getTransactionId(), status);
                    return LocalTransactionState.UNKNOW;
                }

                @Override
                public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                    Integer status = localTrans.get(msg.getTransactionId());
                    if (null != status) {
                        switch (status) {
                            case 0:
                                return LocalTransactionState.UNKNOW;
                            case 1:
                                return LocalTransactionState.COMMIT_MESSAGE;
                            case 2:
                                return LocalTransactionState.ROLLBACK_MESSAGE;
                        }
                    }
                    return LocalTransactionState.COMMIT_MESSAGE;
                }
            });
            producer.start();
            // A消息
            Message message_a = new Message("orders", "order.id.1".getBytes());
            // B消息
            Message message_b = new Message("orders", "order.id.2".getBytes());
            // 批量发送
            List<Message> messages = new ArrayList<>(2);
            messages.add(message_a);
            messages.add(message_b);
            SendResult sendResult = producer.send(messages);
            System.out.println(messages);
            System.out.println(sendResult + " send over!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
