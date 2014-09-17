/**
 * 文件名: RocketMQPushConsumer.java
 * 版    权: Copyright © 2012 - 2014 Bernard. All Rights Reserved
 * 描    述: <描述>
 * 修改人: 461243
 * 修改时间: 2014年9月17日
 * 跟踪单号: <跟踪单号>
 * 修改单号: <修改单号>
 * 修改内容: <修改内容>
 */
package com.rocketmq.test.example;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * @author 461243
 * @version [版本号, 2014年9月17日]
 * @see [想关类/方法]
 * @since [产品/模板版本号]
 */
public class RocketMQPushConsumer
{
    private static final String DEFAULT_MQ_GROUP_NAME = "RocketMQ.Consumer.HelloWorld";
    
    private static final String ISNTANCE_NAME = "RocketMQ.HelloWorld.Consumer";
    
    public static void main(String[] args)
    {
        new RocketMQPushConsumer().consume();
    }
    
    /** 
     * 当前例子是PushConsumer用法，使用方式给用户感觉是消息从RocketMQ服务器推到了应用客户端。<br> 
     * 但是实际PushConsumer内部是使用长轮询Pull方式从MetaQ服务器拉消息，然后再回调用户Listener方法<br> 
     */
    public void consume()
    {
        try
        {
            /** 
             * 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例<br> 
             * 注意：ConsumerGroupName需要由应用来保证唯一 
             */
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(DEFAULT_MQ_GROUP_NAME);
            consumer.setNamesrvAddr("127.0.0.1:9876");
            consumer.setInstanceName(ISNTANCE_NAME);
            
            /** 
             * 订阅指定topic下tags分别等于TagA或TagC或TagD 
             */ 
            consumer.subscribe("RocketMQ1Topic", "['1'] Tags || ['2'] Tags");
            
            /** 
             * 订阅指定topic下所有消息<br> 
             * 注意：一个consumer对象可以订阅多个topic 
             */
            consumer.subscribe("RocketMQ2Topic", "*");
            
            consumer.registerMessageListener(new MessageListenerConcurrently(){

                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context)
                {
                    System.out.println(Thread.currentThread().getName() +" Receive New Messages: " + msgs.size());
                    MessageExt msg = msgs.get(0);
                    String topic = msg.getTopic();
                    if ("RocketMQ1Topic".equals(topic))
                    {
                        String tags = msg.getTags();
                        if (null != tags && "['1'] Tags".equals(tags))
                        {
                            // 执行['1'] Tags的消费
                            System.out.println(tags + "-->" + new String(msg.getBody()));
                        }
                        else if (null != tags && "['2'] Tags".equals(tags))
                        {
                            System.out.println(tags + "-->" + new String(msg.getBody()));
                        }
                        else if (null != tags && "['3'] Tags".equals(tags))
                        {
                            System.out.println(tags + "-->" + new String(msg.getBody()));
                        }
                        else if (null != tags && "['4'] Tags".equals(tags))
                        {
                            System.out.println(tags + "-->" + new String(msg.getBody()));
                        }
                        else if (null != tags && "['5'] Tags".equals(tags))
                        {
                            System.out.println(tags + "-->" + new String(msg.getBody()));
                        }
                    }
                    else if ("RocketMQ2Topic".equals(topic))
                    {
                        System.out.println(msg.getTags() + "-->" + new String(msg.getBody()));
                    }
                    else
                    {
                        System.out.println(msg.getTags() + "-->" + new String(msg.getBody()));
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            
            /** 
             * Consumer对象在使用之前必须要调用start初始化，初始化一次即可<br> 
             */ 
            consumer.start();
            
            System.out.println(ISNTANCE_NAME + " Started.");
        }
        catch (MQClientException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
