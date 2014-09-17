/**
 * 文件名: RocketMQProducer.java
 * 版    权: Copyright © 2012 - 2014 Bernard. All Rights Reserved
 * 描    述: <描述>
 * 修改人: 461243
 * 修改时间: 2014年9月17日
 * 跟踪单号: <跟踪单号>
 * 修改单号: <修改单号>
 * 修改内容: <修改内容>
 */
package com.rocketmq.test.example;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

/**
 * @author 461243
 * @version [版本号, 2014年9月17日]
 * @see [想关类/方法]
 * @since [产品/模板版本号]
 */
public class RocketMQProducer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMQProducer.class);
    
    private static final String DEFAULT_MQ_GROUP_NAME = "RocketMQ.Producer.HellWorld";
    
    private static final String INSTANCE_NAME = "RocketMQ.HellWorld.Producer";
    
    public static void main(String[] args)
    {
        try
        {
            /**  
             * 一个应用创建一个Producer，由应用来维护此对象，可以设置为全局对象或者单例<br>  
             * 注意：ProducerGroupName需要由应用来保证唯一<br>  
             * ProducerGroup这个概念发送普通的消息时，作用不大，但是发送分布式事务消息时，比较关键，  
             * 因为服务器会回查这个Group下的任意一个Producer  
             */
            final DefaultMQProducer producer = new DefaultMQProducer(DEFAULT_MQ_GROUP_NAME);
            producer.setNamesrvAddr("127.0.0.1:9876");
            producer.setInstanceName(INSTANCE_NAME);
            
            /**  
             * Producer对象在使用之前必须要调用start初始化，初始化一次即可<br>  
             * 注意：切记不可以在每次发送消息时，都调用start方法  
             */ 
            
            producer.start();
            
            /**  
             * 下面这段代码表明一个Producer对象可以发送多个topic，多个tag的消息。  
             * 注意：send方法是同步调用，只要不抛异常就标识成功。但是发送成功也可会有多种状态，<br>  
             * 例如消息写入Master成功，但是Slave不成功，这种情况消息属于成功，但是对于个别应用如果对消息可靠性要求极高，<br>  
             * 需要对这种情况做处理。另外，消息可能会存在发送失败的情况，失败重试由应用来处理。  
             */
            
            for (int index = 1; index < 6; index++)
            {
                String topic = "RocketMQ" + index + "Topic";
                String tags = "['"+ index + "'] Tags";
                String body = "['"+ index + "'] RocketMQ HellWorld!";
                try
                {
                    RocketMQProducer.sendMessages(producer, topic, tags, "A0000" + index, body.getBytes(), 1000);
                }
                catch (RemotingException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                catch (MQBrokerException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                catch (InterruptedException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            
            TimeUnit.MILLISECONDS.sleep(10000);
            
            /**  
             * 应用退出时，要调用shutdown来清理资源，关闭网络连接，从MetaQ服务器上注销自己  
             * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法  
             */
            // producer.shutdown();
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
            {
                public void run()
                {
                    producer.shutdown();
                }
            }));

            System.exit(0);
        }
        catch (MQClientException e)
        {
            LOGGER.error("producer not started, because of " + e.getErrorMessage());
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private static void sendMessages(DefaultMQProducer producer, String topic, String tags, String keys, byte[] body, int messageCount) throws MQClientException, RemotingException, MQBrokerException, InterruptedException
    {
        for (int index = 0; index < messageCount; index++)
        {
            Message msg = new Message(topic, tags, keys, body);
            SendResult result = producer.send(msg);
            System.out.println(result);
        }
    }
}
