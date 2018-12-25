package com.olami.producer.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * 生产者
 */
@Service
public class Producer {

    public static final Logger logger = LoggerFactory.getLogger(Producer.class);

    @Value("${spring.rocketmq.namesrvAddr}")
    private String namesrvAddr;

    private final DefaultMQProducer producer = new DefaultMQProducer("grampus-order");

    @PostConstruct
    public void start() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    logger.info("MQ：启动生产者" + namesrvAddr);
                    /**
                     * 设置NameServer地址
                     * 此处应改为实际NameServer地址，多个地址之间用；分隔
                     * NameServer的地址必须有，不一定非得写死在代码里,这里通过配置文件获取
                     */
                    producer.setNamesrvAddr(namesrvAddr);
                    /**
                     * 发送失败重试次数
                     */
                    producer.setRetryTimesWhenSendFailed(10);
                    /**
                     * 调用start()方法启动一个producer实例
                     */
                    producer.start();
                    timer.cancel();
                } catch (MQClientException e) {
                    logger.error("MQ：启动生产者失败：{}-{}",e.getResponseCode(), e.getErrorMessage());
                    e.printStackTrace();
                }
            }
        }, 2000);
    }


    /**
     * 发送消息
     *
     * @param data  消息内容
     * @param topic 主题
     * @param tags  标签 如不需要消费topic下面的所有消息，通过tag进行消息过滤
     * @param keys  唯一主键
     */
    public void sendMessage(String data, @NotNull String topic, String tags, String keys) {
        if(StringUtils.isBlank(data)){
            return;
        }
        try {
            byte[] messageBody = data.getBytes(RemotingHelper.DEFAULT_CHARSET);

            Message mqMsg = new Message(topic, tags, keys, messageBody);

            producer.send(mqMsg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    logger.info("MQ: 生产者发送消息 {}", sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    logger.error(throwable.getMessage(), throwable);
                }
            });
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


    /**
     * 发送消息根据orderId到指定队列，解决消息的顺序问题
     * @param data
     * @param topic
     * @param tags
     * @param keys
     */
    public void sendMessageByOrderId(String data, String topic, String tags, String keys) {
        try {
            JSONObject jsonObject = JSONObject.parseObject(data);

            //获取订单id
            Long orderId = jsonObject.getLong("orderId");
            byte[] messageBody = data.getBytes(RemotingHelper.DEFAULT_CHARSET);
            Message mqMsg = new Message(topic, tags, keys, messageBody);
            producer.send(mqMsg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message msg, Object arg) {
                    //按照订单号发送到固定的队列
                    int index = arg.hashCode() % list.size();
                    index = index < 0 ? -index : index;
                    return list.get(index);
                }
            },orderId,new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    logger.info("MQ: 生产者发送消息 {}", sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    logger.error(throwable.getMessage(), throwable);
                }
            });

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
    @PreDestroy
    public void stop() {
        if (producer != null) {
            producer.shutdown();
            logger.info("MQ：关闭生产者");
        }
    }


}
