package com.olami.consumer8002.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * 消费者的启动必须在spring容器完全启动之后才能开启,
 * 防止应用程序在未启动完成的情况下被消费者堵塞导致启动失败
 * @Author NicholasLiu
 * @Date 2018/12/6 18:06
 **/
@Component
public class ApplicationCompelet implements ApplicationRunner {

    public static final Logger logger = LoggerFactory.getLogger(ApplicationCompelet.class);

    @Autowired
    private RocketConsumerService rocketConsumerService;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        rocketConsumerService.getConsumer().start();
        logger.info("comsumer start success");

    }
}
