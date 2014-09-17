/**
 * 文件名: RocketMQPullConsumer.java
 * 版    权: Copyright © 2012 - 2014 Bernard. All Rights Reserved
 * 描    述: <描述>
 * 修改人: 461243
 * 修改时间: 2014年9月17日
 * 跟踪单号: <跟踪单号>
 * 修改单号: <修改单号>
 * 修改内容: <修改内容>
 */
package com.rocketmq.test.example;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

/**
 * @author 461243
 * @version [版本号, 2014年9月17日]
 * @see [想关类/方法]
 * @since [产品/模板版本号]
 */
public class RocketMQPullConsumer
{
    private static final Map<MessageQueue, Long> queueTable = new HashMap<MessageQueue, Long>();

    private static final String DEFAULT_MQ_GROUP_NAME = "RocketMQ.Consumer.HelloWorld";
    
    private static final String ISNTANCE_NAME = "RocketMQ.HelloWorld.Consumer";
    
    public static void main(String[] args)
    {
        new RocketMQPullConsumer().consume();
    }
    
    public void consume()
    {
        try
        {
            /** 
             * 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例<br> 
             * 注意：ConsumerGroupName需要由应用来保证唯一 
             */
            DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(DEFAULT_MQ_GROUP_NAME);
            consumer.setNamesrvAddr("127.0.0.1:9876");
            consumer.setInstanceName(ISNTANCE_NAME);
            
            consumer.start();
            
            Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues("RocketMQ5Topic");
            for (MessageQueue queue : queues)
            {
                SINGLE_MQ: while (true)
                {
                    try
                    {
                        PullResult result = consumer.pullBlockIfNotFound(queue, null, RocketMQPullConsumer.getMessageQueueOffset(queue), 32);
                        System.out.println(result);
                        RocketMQPullConsumer.putMessageQueueOffset(queue, result.getNextBeginOffset());
                        switch (result.getPullStatus())
                        {
                            case FOUND:
                            {
                                //TODO 
                                break;
                            }
                            case NO_MATCHED_MSG:
                            {
                                break;
                            }
                            case NO_NEW_MSG:
                            {
                                break SINGLE_MQ;
                            }
                            case OFFSET_ILLEGAL:
                            {
                                break;
                            }
                            default:
                            {
                                break;
                            }
                        }
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
            }
            
            consumer.shutdown();
        }
        catch (MQClientException e)
        {
            e.printStackTrace();
        }
    }
    
    private static void putMessageQueueOffset(MessageQueue queue, long offset)
    {
        queueTable.put(queue, offset);
    }
    
    private static long getMessageQueueOffset(MessageQueue queue)
    {
        if (queueTable.containsKey(queue))
        {
            Long offset = queueTable.get(queue);
            if (null != offset)
            {
                return offset;
            }
        }
        return 0;
    }
}
