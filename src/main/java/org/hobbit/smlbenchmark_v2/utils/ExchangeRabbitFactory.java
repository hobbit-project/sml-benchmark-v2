package org.hobbit.smlbenchmark_v2.utils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.hobbit.core.data.RabbitQueue;
import org.hobbit.core.rabbit.RabbitQueueFactory;

import java.io.IOException;
import java.util.Map;


public class ExchangeRabbitFactory implements RabbitQueueFactory {
    private final Connection connection;

    public ExchangeRabbitFactory(Connection connection) {
        this.connection = connection;
    }

    public RabbitQueue createDefaultRabbitExchange(String name) throws IOException {
        return this.createDefaultRabbitExchange(name, this.createChannel());
    }

    public RabbitQueue createExchangeBoundRabbitQueue(String name) throws IOException {
        return this.createExchangeBoundRabbitQueue(name, this.createChannel());
    }

    public RabbitQueue createDefaultRabbitQueue(String name) throws IOException {
        return this.createDefaultRabbitQueue(name, this.createChannel());
    }

    public Connection getConnection() {
        return this.connection;
    }

    public void close() throws IOException {
        if (this.connection != null) {
            try {
                this.connection.close();
            } catch (Exception var2) {
                ;
            }
        }

    }

    @Override
    public RabbitQueue createDefaultRabbitQueue(String name, Channel channel) throws IOException {
        channel.queueDeclare(name, false, false, true, null);
        return new RabbitQueue(channel, name);
    }


    public RabbitQueue createExchangeBoundRabbitQueue(String exchangeQueueName, Channel channel) throws IOException {

        channel.exchangeDeclare(exchangeQueueName, "fanout", false, true, (Map)null);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, exchangeQueueName, "");

        return new RabbitQueue(channel, queueName);
    }

    public RabbitQueue createDefaultRabbitExchange(String exchangeQueueName, Channel channel) throws IOException {
        channel.exchangeDeclare(exchangeQueueName, "fanout", false, true, (Map)null);
        return new RabbitQueue(channel, exchangeQueueName);
    }

    public Channel createChannel() throws IOException {

        return this.connection.createChannel();
    }
}