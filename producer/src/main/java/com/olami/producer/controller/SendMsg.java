package com.olami.producer.controller;

import com.alibaba.fastjson.JSONObject;
import com.olami.producer.entity.OrderDemo;
import com.olami.producer.service.Producer;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.text.SimpleDateFormat;
import java.util.*;

@Controller
public class SendMsg {

    public static final org.slf4j.Logger logger = LoggerFactory.getLogger(Producer.class);

    @Autowired
    private Producer producer;

    @Value("${spring.rocketmq.topicsAndTags}")
    private String topic;

    @ResponseBody
    @GetMapping("/send")
    public String send() {

        String[] tags = new String[] { "TagA", "TagC", "TagD" };

        List<OrderDemo> orderList =  buildOrders();
        String dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        for (int i = 0; i < 10; i++) {
            //String data = dateStr + " Hello RocketMQ " + orderList.get(i);
            OrderDemo orderDemo = orderList.get(i);
            String data = JSONObject.toJSONString(orderDemo);
            String tag = tags[i % tags.length];
            String key = "KEY" + i;
            producer.sendMessageByOrderId(data, topic,tag,key);
        }

        return "success";
    }


    /**
     * 生成模拟订单数据
     */
    private List<OrderDemo> buildOrders() {
        List<OrderDemo> orderList = new ArrayList<OrderDemo>();

        OrderDemo orderDemo = new OrderDemo();
        orderDemo.setOrderId(15103111039L);
        orderDemo.setDesc("创建");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setOrderId(15103111065L);
        orderDemo.setDesc("创建");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setOrderId(15103111039L);
        orderDemo.setDesc("付款");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setOrderId(15103117235L);
        orderDemo.setDesc("创建");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setOrderId(15103111065L);
        orderDemo.setDesc("付款");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setOrderId(15103117235L);
        orderDemo.setDesc("付款");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setOrderId(15103111065L);
        orderDemo.setDesc("完成");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setOrderId(15103111039L);
        orderDemo.setDesc("推送");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setOrderId(15103117235L);
        orderDemo.setDesc("完成");
        orderList.add(orderDemo);

        orderDemo = new OrderDemo();
        orderDemo.setOrderId(15103111039L);
        orderDemo.setDesc("完成");
        orderList.add(orderDemo);

        return orderList;
    }




}
