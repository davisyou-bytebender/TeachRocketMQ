package org.apache.rocketmq.example.async;

import java.io.UnsupportedEncodingException;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author davisyou
 * @version 1.0
 * @date 2022/11/23 2:53 PM
 */
public class AsyncProducer {
    private final static String PRODUCER_GROUP_NAME = "async_producer_group";

    public static void main(String[] args)
        throws UnsupportedEncodingException, MQClientException, RemotingException, InterruptedException {
        // 创建消息生产者并指定生产者组名
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer(PRODUCER_GROUP_NAME);

        // 设置NameServer的地址
        defaultMQProducer.setNamesrvAddr("localhost:19876");

        // 启动Producer实例
        defaultMQProducer.start();

        for (int i = 0; i < 10; i++) {
            int index = i;
            Message message = new Message(
                /* topic*/
                "async_message",
                /* tag */
                "TagA",
                /* Message Body */
                ("Async Message " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            // 发送消息到Broker
            defaultMQProducer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index,
                        sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable throwable) {
                    System.out.printf("%-10d Exception %s %n", index, throwable);
                    throwable.printStackTrace();
                }
            });
        }

        // 如果不发送消息，关闭Producer实例
        defaultMQProducer.shutdown();
    }
}
