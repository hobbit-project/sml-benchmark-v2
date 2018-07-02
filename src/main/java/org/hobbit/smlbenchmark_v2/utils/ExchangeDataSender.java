package org.hobbit.smlbenchmark_v2.utils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.ConfirmListener;
import org.apache.commons.io.IOUtils;
import org.hobbit.core.data.RabbitQueue;
import org.hobbit.core.rabbit.DataSender;
import org.hobbit.core.rabbit.RabbitQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

public class ExchangeDataSender implements DataSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExchangeDataSender.class);
    private static final int DEFAULT_MESSAGE_BUFFER_SIZE = 1000;
    private static final int DEFAULT_DELIVERY_MODE = 2;
    private RabbitQueue queue;
    private final int deliveryMode;
    private final DataSenderConfirmHandler confirmHandler;

    protected ExchangeDataSender(RabbitQueue queue, int deliveryMode, int messageConfirmBuffer) {
        this.queue = queue;
        this.deliveryMode = deliveryMode;
        if (messageConfirmBuffer > 0) {
            try {
                this.queue.channel.confirmSelect();
            } catch (Exception var5) {
                LOGGER.error("Exception whily trying to enable confirms. The sender might work, but it won't guarantee that messages are received.");
                this.confirmHandler = null;
                return;
            }

            this.confirmHandler = new DataSenderConfirmHandler(messageConfirmBuffer);
            this.queue.channel.addConfirmListener(this.confirmHandler);
        } else {
            this.confirmHandler = null;
        }

    }

    public void sendData(byte[] data) throws IOException {
        this.sendData(data, new com.rabbitmq.client.AMQP.BasicProperties.Builder());
    }

    protected void sendData(byte[] data, com.rabbitmq.client.AMQP.BasicProperties.Builder probBuilder) throws IOException {
        probBuilder.deliveryMode(this.deliveryMode);
        if (this.confirmHandler != null) {
            this.confirmHandler.sendDataWithConfirmation(probBuilder.build(), data);
        } else {
            this.sendData(probBuilder.build(), data);
        }

    }

    protected void sendData(AMQP.BasicProperties properties, byte[] data) throws IOException {
        //this.queue.channel.basicPublish("", this.queue.name, properties, data);
        this.queue.channel.basicPublish(this.queue.name, "", properties, data);
    }

    public void closeWhenFinished() {
        if (this.confirmHandler != null) {
            try {
                this.confirmHandler.waitForConfirms();
            } catch (InterruptedException var2) {
                LOGGER.warn("Exception while waiting for confirmations. It can not be guaranteed that all messages have been consumed.", var2);
            }
        }

        try {
            for(int check = 0; check < 5; Thread.sleep(200L)) {
                if (this.queue.messageCount() > 0L) {
                    check = 0;
                } else {
                    ++check;
                }
            }
        } catch (AlreadyClosedException var3) {
            LOGGER.info("The queue is already closed. Assuming that all messages have been consumed.");
        } catch (Exception var4) {
            LOGGER.warn("Exception while trying to check whether all messages have been consumed. It will be ignored.", var4);
        }

        this.close();
    }

    public void close() {
        IOUtils.closeQuietly(this.queue);
    }

    public static ExchangeDataSender.Builder builder() {
        return new ExchangeDataSender.Builder();
    }

    protected class DataSenderConfirmHandler implements ConfirmListener {
        private final Semaphore maxBufferedMessageCount;
        private final SortedMap<Long, ExchangeDataSender.Message> unconfirmedMsgs = Collections.synchronizedSortedMap(new TreeMap());
        private int successfullySubmitted = 0;

        public DataSenderConfirmHandler(int messageConfirmBuffer) {
            this.maxBufferedMessageCount = new Semaphore(messageConfirmBuffer);
        }

        public synchronized void sendDataWithConfirmation(AMQP.BasicProperties properties, byte[] data) throws IOException {
            try {
                ExchangeDataSender.LOGGER.trace("{}\tavailable\t{}", ExchangeDataSender.this.toString(), this.maxBufferedMessageCount.availablePermits());
                this.maxBufferedMessageCount.acquire();
            } catch (InterruptedException var6) {
                throw new IOException("Interrupted while waiting for free buffer to store the message before sending.", var6);
            }

            SortedMap var3 = this.unconfirmedMsgs;
            synchronized(this.unconfirmedMsgs) {
                this.sendData_unsecured(new ExchangeDataSender.Message(properties, data));
            }
        }

        private void sendData_unsecured(ExchangeDataSender.Message message) throws IOException {
            synchronized(ExchangeDataSender.this.queue.channel) {
                long sequenceNumber = ExchangeDataSender.this.queue.channel.getNextPublishSeqNo();
                ExchangeDataSender.LOGGER.trace("{}\tsending\t{}", ExchangeDataSender.this.toString(), sequenceNumber);
                this.unconfirmedMsgs.put(sequenceNumber, message);

                try {
                    ExchangeDataSender.this.sendData(message.properties, message.data);
                } catch (IOException var7) {
                    this.unconfirmedMsgs.remove(sequenceNumber);
                    this.maxBufferedMessageCount.release();
                    throw var7;
                }

            }
        }

        public void handleAck(long deliveryTag, boolean multiple) throws IOException {
            SortedMap var4 = this.unconfirmedMsgs;
            synchronized(this.unconfirmedMsgs) {
                if (multiple) {
                    SortedMap<Long, ExchangeDataSender.Message> negativeMsgs = this.unconfirmedMsgs.headMap(deliveryTag + 1L);
                    int ackMsgCount = negativeMsgs.size();
                    negativeMsgs.clear();
                    this.maxBufferedMessageCount.release(ackMsgCount);
                    this.successfullySubmitted += ackMsgCount;
                    ExchangeDataSender.LOGGER.trace("{}\tack\t{}+\t{}", new Object[]{ExchangeDataSender.this.toString(), deliveryTag, this.maxBufferedMessageCount.availablePermits()});
                } else {
                    this.unconfirmedMsgs.remove(deliveryTag);
                    ++this.successfullySubmitted;
                    this.maxBufferedMessageCount.release();
                    ExchangeDataSender.LOGGER.trace("{}\tack\t{}\t{}", new Object[]{ExchangeDataSender.this.toString(), deliveryTag, this.maxBufferedMessageCount.availablePermits()});
                }

            }
        }

        public void handleNack(long deliveryTag, boolean multiple) throws IOException {
            SortedMap var4 = this.unconfirmedMsgs;
            synchronized(this.unconfirmedMsgs) {
                ExchangeDataSender.LOGGER.trace("nack\t{}{}", deliveryTag, multiple ? "+" : "");
                if (multiple) {
                    SortedMap<Long, ExchangeDataSender.Message> negativeMsgs = this.unconfirmedMsgs.headMap(deliveryTag + 1L);
                    ExchangeDataSender.Message[] messageToResend = (ExchangeDataSender.Message[])negativeMsgs.values().toArray(new ExchangeDataSender.Message[negativeMsgs.size()]);
                    negativeMsgs.clear();

                    for(int i = 0; i < messageToResend.length; ++i) {
                        this.sendData_unsecured(messageToResend[i]);
                    }
                } else if (this.unconfirmedMsgs.containsKey(deliveryTag)) {
                    ExchangeDataSender.Message message = (ExchangeDataSender.Message)this.unconfirmedMsgs.remove(deliveryTag);
                    this.sendData_unsecured(message);
                } else {
                    ExchangeDataSender.LOGGER.warn("Got a negative acknowledgement (nack) for an unknown message. It will be ignored.");
                }

            }
        }

        public void waitForConfirms() throws InterruptedException {
            while(true) {
                SortedMap var1 = this.unconfirmedMsgs;
                synchronized(this.unconfirmedMsgs) {
                    if (this.unconfirmedMsgs.size() == 0) {
                        ExchangeDataSender.LOGGER.trace("sent {} messages.", this.successfullySubmitted);
                        return;
                    }
                }

                Thread.sleep(200L);
            }
        }
    }

    protected static class Message {
        public AMQP.BasicProperties properties;
        public byte[] data;

        public Message(AMQP.BasicProperties properties, byte[] data) {
            this.properties = properties;
            this.data = data;
        }
    }

    public static class Builder {
        protected static final String QUEUE_INFO_MISSING_ERROR = "There are neither a queue nor a queue name and a queue factory provided for the DataSender. Either a queue or a name and a factory to create a new queue are mandatory.";
        protected RabbitQueue queue;
        protected String queueName;
        protected ExchangeRabbitFactory factory;
        protected int messageConfirmBuffer = 1000;
        protected int deliveryMode = 2;

        public Builder() {
        }

        public ExchangeDataSender.Builder queue(RabbitQueue queue) {
            this.queue = queue;
            return this;
        }

        public ExchangeDataSender.Builder queue(ExchangeRabbitFactory factory, String queueName) {
            this.factory = factory;
            this.queueName = queueName;
            return this;
        }

        public ExchangeDataSender.Builder messageBuffer(int messageConfirmBuffer) {
            this.messageConfirmBuffer = messageConfirmBuffer;
            return this;
        }

        public ExchangeDataSender.Builder deliveryMode(int deliveryMode) {
            this.deliveryMode = deliveryMode;
            return this;
        }

        public ExchangeDataSender build() throws IllegalStateException, IOException {
            if (this.queue == null) {
                if (this.queueName == null || this.factory == null) {
                    throw new IllegalStateException("There are neither a queue nor a queue name and a queue factory provided for the DataSender. Either a queue or a name and a factory to create a new queue are mandatory.");
                }

                this.queue = this.factory.createDefaultRabbitExchange(this.queueName);
            }

            return new ExchangeDataSender(this.queue, this.deliveryMode, this.messageConfirmBuffer);
        }
    }
}
