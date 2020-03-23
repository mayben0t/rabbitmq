package producer;

import api.MqConfigInfo;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;


public class RabbitProducer extends MqConfigInfo {
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(IP_ADDRESS);
        factory.setPort(PORT);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME,"direct",true,false,null);
        channel.queueDeclare(QUEUE_NAME,true,false,false,null);
        channel.queueBind(QUEUE_NAME,EXCHANGE_NAME,ROUTING_KEY);
        String message = "你别再靠近我了";
        channel.basicPublish(EXCHANGE_NAME,ROUTING_KEY,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes());


        AtomicInteger integer = new AtomicInteger(0);
    List<Thread> threadList = new ArrayList<>();
        long l = System.currentTimeMillis();
        for(int i = 0;i<10000;i++){
            Thread thread = new Thread(()->{
                try {
                    channel.txSelect();
                    channel.basicPublish(EXCHANGE_NAME,ROUTING_KEY,new AMQP.BasicProperties.Builder().contentType("text/plain")
                                    .deliveryMode(2)
                                    .priority(1)
                                    .userId("guest")
                                    .build(),
                            "haha".getBytes());
                    System.out.println("currentThread "+Thread.currentThread().getName()+" 发送成功");
                    TimeUnit.SECONDS.sleep(10);
                    integer.getAndIncrement();
//                    channel.txCommit();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            },"name:"+i);
            threadList.add(thread);

        }

        threadList.forEach(Thread::start);
        while (integer.get()!=10000){

        }

        long l1 = System.currentTimeMillis();
        System.out.println("1w条耗时:"+(l1-l)+"ms");
        //关闭资源
//        channel.close();
//        connection.close();
    }
}
