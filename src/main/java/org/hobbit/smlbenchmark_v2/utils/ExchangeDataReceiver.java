package org.hobbit.smlbenchmark_v2.utils;

import com.rabbitmq.client.QueueingConsumer;
import org.apache.commons.io.IOUtils;
import org.hobbit.core.data.RabbitQueue;
import org.hobbit.core.rabbit.DataHandler;
import org.hobbit.core.rabbit.DataReceiver;
import org.hobbit.utils.TerminatableRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExchangeDataReceiver implements DataReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(org.hobbit.core.rabbit.DataReceiverImpl.class);
    private static final int DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES = 50;
    protected RabbitQueue queue;
    private int errorCount = 0;
    private DataHandler dataHandler;
    private ExecutorService executor = null;
    private TerminatableRunnable receiverTask;
    private Thread receiverThread;

    protected ExchangeDataReceiver(RabbitQueue queue, DataHandler handler, int maxParallelProcessedMsgs) throws IOException {

        //        String queueName = evalStorageTaskGenChannel.queueDeclare().getQueue();
//        evalStorageTaskGenChannel.queueBind(queueName, exchangeQueueName, "");
//
//        exchangeQueueConsumer = new QueueingConsumer(evalStorageTaskGenChannel);
//        evalStorageTaskGenChannel.basicConsume(queueName, true, exchangeQueueConsumer);

        this.queue = queue;
        this.dataHandler = handler;

        QueueingConsumer consumer = new QueueingConsumer(queue.channel);
        queue.channel.basicConsume(queue.name, true, consumer);
        queue.channel.basicQos(maxParallelProcessedMsgs);

        this.executor = Executors.newFixedThreadPool(maxParallelProcessedMsgs);
        this.receiverTask = this.buildMsgReceivingTask(consumer);
        this.receiverThread = new Thread(this.receiverTask);
        this.receiverThread.start();
    }

    public DataHandler getDataHandler() {
        return this.dataHandler;
    }

    public synchronized void increaseErrorCount() {
        ++this.errorCount;
    }

    public int getErrorCount() {
        return this.errorCount;
    }

    public RabbitQueue getQueue() {
        return this.queue;
    }

    protected ExecutorService getExecutor() {
        return this.executor;
    }

    public void closeWhenFinished() {
        LOGGER.debug("receiverTask.terminate()");
        this.receiverTask.terminate();

        LOGGER.debug("receiverThread.join()");
        try {
            this.receiverThread.join();
        } catch (Exception var3) {
            LOGGER.error("Exception while waiting for termination of receiver task. Closing receiver.", var3);
        }

        LOGGER.debug("executor.shutdown()");
        this.executor.shutdown();

        try {
            this.executor.awaitTermination(1L, TimeUnit.DAYS);
        } catch (InterruptedException var2) {
            LOGGER.error("Exception while waiting for termination. Closing receiver.", var2);
        }

        this.close();
    }

    public void close() {
        IOUtils.closeQuietly(this.queue);
        if (this.executor != null && !this.executor.isShutdown()) {
            this.executor.shutdownNow();
        }

    }

    public static Builder builder() {
        return new Builder();
    }

    protected TerminatableRunnable buildMsgReceivingTask(QueueingConsumer consumer) {
        return new MsgReceivingTask(consumer);
    }

    protected Runnable buildMsgProcessingTask(QueueingConsumer.Delivery delivery) {
        return new MsgProcessingTask(delivery);
    }

    public static class Builder {
        protected static final String QUEUE_INFO_MISSING_ERROR = "There are neither a queue nor a queue name and a queue factory provided for the DataReceiver. Either a queue or a name and a factory to create a new queue are mandatory.";
        protected static final String DATA_HANDLER_MISSING_ERROR = "The necessary data handler has not been provided for the DataReceiver.";
        protected DataHandler dataHandler;
        protected RabbitQueue queue;
        protected String queueName;
        protected int maxParallelProcessedMsgs = 50;
        protected ExchangeRabbitFactory factory;

        public Builder() {
        }

        public Builder dataHandler(DataHandler dataHandler) {
            this.dataHandler = dataHandler;
            return this;
        }

        public Builder queue(RabbitQueue queue) {
            this.queue = queue;
            return this;
        }

        public Builder queue(ExchangeRabbitFactory factory, String queueName) {
            this.factory = factory;
            this.queueName = queueName;
            return this;
        }

        public Builder maxParallelProcessedMsgs(int maxParallelProcessedMsgs) {
            this.maxParallelProcessedMsgs = maxParallelProcessedMsgs;
            return this;
        }

        public ExchangeDataReceiver build() throws IllegalStateException, IOException {
            if (this.dataHandler == null) {
                throw new IllegalStateException("The necessary data handler has not been provided for the DataReceiver.");
            } else {
                if (this.queue == null) {
                    if (this.queueName == null || this.factory == null) {
                        throw new IllegalStateException("There are neither a queue nor a queue name and a queue factory provided for the DataReceiver. Either a queue or a name and a factory to create a new queue are mandatory.");
                    }

                    this.queue = this.factory.createExchangeBoundRabbitQueue(this.queueName);
                }

                try {
                    return new ExchangeDataReceiver(this.queue, this.dataHandler, this.maxParallelProcessedMsgs);
                } catch (IOException var2) {
                    IOUtils.closeQuietly(this.queue);
                    throw var2;
                }
            }
        }
    }

    protected class MsgProcessingTask implements Runnable {
        private QueueingConsumer.Delivery delivery;

        public MsgProcessingTask(QueueingConsumer.Delivery delivery) {
            this.delivery = delivery;
        }

        public void run() {
            try {
                ExchangeDataReceiver.this.dataHandler.handleData(this.delivery.getBody());
            } catch (Throwable var2) {
                LOGGER.error("Uncatched throwable inside the data handler.", var2);
            }

        }
    }

    protected class MsgReceivingTask implements TerminatableRunnable {
        private QueueingConsumer consumer;
        private boolean runFlag = true;

        public MsgReceivingTask(QueueingConsumer consumer) {
            this.consumer = consumer;
        }

        public void run() {
            int count = 0;
            QueueingConsumer.Delivery delivery = null;

            while(this.runFlag || ExchangeDataReceiver.this.queue.messageCount() > 0L || delivery != null) {
                try {
                    delivery = this.consumer.nextDelivery(3000L);
                } catch (Exception var4) {
                   // LOGGER.error("Exception while waiting for delivery.", var4);
                    ExchangeDataReceiver.this.increaseErrorCount();
                }

                if (delivery != null) {
                    try {
                        ExchangeDataReceiver.this.executor.submit(ExchangeDataReceiver.this.buildMsgProcessingTask(delivery));
                        ++count;
                    }
                    catch (Exception e){

                    }

                }
            }

            LOGGER.debug("Receiver task terminates after receiving {} messages.", count);
        }

        public void terminate() {
            this.runFlag = false;
        }

        public boolean isTerminated() {
            return !this.runFlag;
        }
    }
}