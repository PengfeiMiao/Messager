package com.jincou.rocketmq.controller;


import com.jincou.rocketmq.jms.JmsConfig;
import com.jincou.rocketmq.jms.Producer;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@RestController
public class Controller {

    @Autowired
    Producer producer ;

    @GetMapping("/message")
    public  void  message() throws Exception {
        //同步
        sync();
        //异步
        async();
        //单项发送
        oneWay();
    }

    @GetMapping("/timetask")
    public String ScheduledExecutorService() {
        //
        ScheduledExecutorService service = Executors.newScheduledThreadPool(10);
        service.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                //异步
                try {
                    //设置随机延时
                    int delay = new Random().nextInt(5*1000 + 1) + 5*1000;
                    Thread.sleep(delay);
                    async();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                log.info("定时任务执行：" + new Date());
            }
        }, 1, 1, TimeUnit.SECONDS);//首次延迟1秒，之后每1秒执行一次

        log.info("定时任务启动：" + new Date());
        return "ScheduledExecutorService!";
    }

    /**
     * 1、同步发送消息
     */
    private  void sync() throws Exception {
        //创建消息
        Message message = new Message(JmsConfig.TOPIC, ("  同步发送  ").getBytes());
        //同步发送消息
        SendResult sendResult = producer.getProducer().send(message);
        log.info("Product-同步发送-Product信息={}", sendResult);
    }
    /**
     * 2、异步发送消息
     */
    private  void async() throws Exception {
        //创建消息
        Message message = new Message(JmsConfig.TOPIC, ("  异步发送  ").getBytes());
        //异步发送消息
        producer.getProducer().send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("Product-异步发送-输出信息={}", sendResult);
            }
            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                //补偿机制，根据业务情况进行使用，看是否进行重试
            }
        });
    }
    /**
     * 3、单项发送消息
     */
    private  void oneWay() throws Exception {
        //创建消息
        Message message = new Message(JmsConfig.TOPIC, (" 单项发送 ").getBytes());
        //同步发送消息
        producer.getProducer().sendOneway(message);
    }

}
