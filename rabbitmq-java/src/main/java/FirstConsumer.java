import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * @Author FanXH
 * @Date 2023/6/20 15:52
 * @Description
 */
public class FirstConsumer {
    private static final String HOST_NAME="10.211.55.12";
    private static final int HOST_PORT=5672;
    private static final String QUEUE_NAME="test2";
    public static final String USER_NAME="admin";
    public static final String PASSWORD="admin";
    public static final String VIRTUAL_HOST="/";


    public static void main(String[] args) throws Exception{
        //创建链接工厂
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST_NAME);
        factory.setPort(HOST_PORT);
        factory.setUsername(USER_NAME);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost(VIRTUAL_HOST);
        //获取链接
        Connection connection = factory.newConnection();
        //获取信道
        final Channel channel = connection.createChannel();

        /**
         * 声明一个对列。
         * 几个参数依次为: 队列名，durable是否实例化;exclusive:是否独占; autoDelete:是否自动删除;arguments:参数
         * 这几个参数跟创建队列的页面是一致的。
         * 如果Broker上没有队列，那么就会自动创建队列。
         * 但是如果Broker上已经由了这个队列。那么队列的属性必须匹配，否则会报错。
         */
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);

        //每个worker同时最多只处理一个消息
        channel.basicQos(1);




        //回调函数，处理接收到的消息
        /**
         * channel暂时不用过多纠结于实现细节，注意梳理整体实现流程。
         * 执行这个应用程序后，就会在RabbitMQ上新创建一个test2的队列(如果你之前没有创建过的话)，并且启 动一个消费者，处理test2队列上的消息。
         * 这时，我们可以从管理平台页面上往test2队列发送一条消息，这 个消费者程序就会及时消费消息。
         * 然后在管理平台的Connections和Channels里就能看到这个消费者程序与RabbitMQ建立的一个 Connection连接与一个Channel通道。
         */
        Consumer myconsumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("========================");
                String routingKey = envelope.getRoutingKey();
                System.out.println("routingKey >"+routingKey);
                String contentType = properties.getContentType();
                System.out.println("contentType >"+contentType);
                long deliveryTag = envelope.getDeliveryTag();
                System.out.println("deliveryTag >"+deliveryTag);
                System.out.println("content:"+new String(body,"UTF-8"));
                // (process the message components here ...)
                channel.basicAck(deliveryTag, false);
            } };

        //从test1队列接收消息
        channel.basicConsume(QUEUE_NAME, myconsumer);
    }
}
