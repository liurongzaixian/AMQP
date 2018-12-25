package com.olami.consumer8001.service;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;

/**
 *
 * 消费者
 * @Author NicholasLiu
 * @Date 2018/12/6 15:55
 **/
@Service
public class RocketConsumerService {

    public static final Logger logger = LoggerFactory.getLogger(RocketConsumerService.class);

    /**
     * 服务地址
     */
    @Value("${spring.rocketmq.namesrvAddr}")
    private String namesrvAddr;
    /**
     * 组名
     */
    @Value("${spring.rocketmq.cousumerGroupName}")
    private String cousumerGroupName;
    /**
     * 批量消费数量
     */
    @Value("${spring.rocketmq.messageBatchMaxSize}")
    private int messageBatchMaxSize;
    /**
     * topic和tags
     */
    @Value("${spring.rocketmq.topicsAndTags}")
    private List<String> topicsAndTags;
    /**
     * 默认的消费者
     */
    private final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();

    /**
     * 初始化以及开启消费者,启动监听
     */
    @PostConstruct
    public void start() {
        try {
            logger.info("MQ：启动消费者");
            /**
             * NameServer的地址和端口，多个逗号分隔开，达到消除单点故障的目的
             */
            consumer.setNamesrvAddr(namesrvAddr);
            /**
             * 设置群组名称
             */
            consumer.setConsumerGroup(cousumerGroupName);
            /**
             *  批量消费的数量
             *  1.如果consumer先启动，producer发一条consumer消费一条
             *  2.如果consumer后启动，mq堆积数据，consumer每次消费设置的数量
             */
            consumer.setConsumeMessageBatchMaxSize(messageBatchMaxSize);
            /**
             * consumer的消费策略
             * CONSUME_FROM_LAST_OFFSET 默认策略，从该队列最尾开始消费，即跳过历史消息
             * CONSUME_FROM_FIRST_OFFSET 从队列最开始开始消费，即历史消息（还储存在broker的）全部消费一遍
             * CONSUME_FROM_TIMESTAMP 从某个时间点开始消费，和setConsumeTimestamp()配合使用，默认是半个小时以前
             */
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            /**
             * 消费模式：
             * 1.CLUSTERING：集群，默认
             *   同一个Group里每个consumer只消费订阅消息的一部分内容，也就是同一groupName,所有消费的内容加起来才是订阅topic内容的整体，达到负载均衡的目的
             * 2.BROADCASTING:广播模式
             *   同一个Group里每个consumer都能消费到所订阅topic的全部消息，也就是一个消息会被分发多次，被多个consumer消费
             *   广播消息只发送一次，没有重试
             */
            consumer.setMessageModel(MessageModel.CLUSTERING);
            // 订阅所有的topic以及tags
            for (String topics: topicsAndTags) {
                String[] topiclist = topics.split("~");
                consumer.subscribe(topiclist[0],topiclist[1]);
            }

            // 注册消息监听器
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                /**
                 * 消费消息
                 * @param msgs
                 * @param context
                 * @return
                 */
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    int index = 0;
                    try {
                        for (; index < msgs.size(); index++) {
                            MessageExt msg = msgs.get(index);
                            String messageBody = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                            logger.info("8001 receive msg：" + messageBody);
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                        /**
                         * 重试机制(consumer),仅限于CLUSTERING模式
                         * 1.exception的情况，一般重复16次 10s、30s、1分钟、2分钟、3分钟等等
                         *   获取重试次数：msgs.get(0).getReconsumeTimes()
                         * 2.超时的情况，这种情况MQ会无限制的发送给消费端
                         *   就是由于网络的情况，MQ发送数据之后，Consumer端并没有收到导致超时。也就是消费端没有给我返回return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;这样的就认为没有到达Consumer端
                         */
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    } finally {
                        logger.info("index==" + index + "  msgs size=" + msgs.size());
                        /**
                         * 如果index小于本组数据的数量,则认为消费失败,记录一下失败消息的索引,以便重新消费,避免重复消费
                         */
                        if (index < msgs.size()) {
                            context.setAckIndex(index + 1);
                        }
                    }
                    /**
                     * 返回消费状态：
                     * CONSUME_SUCCESS 消费成功
                     * RECONSUME_LATER 消费失败，需要稍后重新消费
                     */
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            //consumer.start();
        } catch (MQClientException e) {
            logger.error("MQ：启动消费者失败：{}-{}", e.getResponseCode(), e.getErrorMessage());
        }

    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }

    @PreDestroy
    public void stop() {
        if (consumer != null) {
            consumer.shutdown();
            logger.error("MQ：关闭消费者");
        }
    }


}
