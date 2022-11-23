package org.apache.rocketmq.example.sync;

import java.io.UnsupportedEncodingException;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author davisyou
 * @version 1.0
 * @date 2022/11/23 2:52 PM
 */
public class SyncProducer {

    private final static String PRODUCER_GROUP_NAME = "sync_producer_group";

    public static void main(String[] args)
        throws MQClientException, MQBrokerException, RemotingException, InterruptedException,
        UnsupportedEncodingException {
        // 创建消息生产者并指定生产者组名
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer(PRODUCER_GROUP_NAME);

        // 设置NameServer的地址
        defaultMQProducer.setNamesrvAddr("localhost:19876");

        // 启动Producer实例
        defaultMQProducer.start();

        for (int i = 0; i < 10; i++) {
            Message message = new Message(
                /* topic*/
                "sync_message",
                /* tag */
                "TagA",
                /* Message Body */
                ("Sync Message " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            // 发送消息到Broker
            SendResult sendResult = defaultMQProducer.send(message);
            // 通过sendResult返回消息是否成功送达
            System.out.printf("%s%n", sendResult);
        }

        // 如果不发送消息，关闭Producer实例
        defaultMQProducer.shutdown();
    }
}
