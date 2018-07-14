/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.stream.pulsar;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.stream.StreamAdapter;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * Server that subscribes to topic messages from Kafka broker and streams its to key-value pairs into {@link
 * IgniteDataStreamer} instance.
 * <p>
 * Uses Kafka's High Level Consumer API to read messages from Kafka. Ò
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example">Consumer Consumer Group
 * Example</a>
 */
public class PulsarStreamer extends StreamAdapter<Message, String, String> {

    /** SubscriptionType. */
    private static final SubscriptionType DFLT_SUBSCRIPTION_TYPE = SubscriptionType.Exclusive;
    /** OperationTimeout. */
    private static final int DELT_OPERATION_TIMEOUT = 30;
    /** AckTimeout. */
    private static final long DELT_ACK_TIMEOUT = 10;
    /** Logger. */
    private IgniteLogger log;
    /** Executor used to submit pulsar streams. */
    private ExecutorService executor;
    /** Service URL. */
    private String serviceUrl;
    /** Topic. */
    private String topic;
    /** SubscriptionName. */
    private String subscription;
    /** SubscriptionType. */
    private SubscriptionType subscriptionType = DFLT_SUBSCRIPTION_TYPE;

//
//    /** Number of threads to process kafka streams. */
//    private int threads;
//
//    /** Kafka consumer config. */
//    private ConsumerConfig consumerCfg;
//
    /** Pulsar consumer. */
    private Consumer consumer;

    /** Pulsar client. */
    private PulsarClient client;

    /** OperationTimeout. */
    private int operationTimeout = DELT_OPERATION_TIMEOUT;

    /** AckTimeout. */
    private long ackTimeout = DELT_ACK_TIMEOUT;

    /** Stopped. */
    private volatile boolean stopped;

    /**
     * Sets the service URL.
     *
     * @param serviceUrl Service URL.
     */
    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    /**
     * Sets the topic name.
     *
     * @param topic Topic name.
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * Sets the subscription name.
     *
     * @param subscription Subscription name.
     */
    public void setSubscription(String subscription) {
        this.subscription = subscription;
    }

    /**
     * Sets the subscription type.
     *
     * @param subscriptionType Subscription type.
     */
    public void setSubscriptionType(SubscriptionType subscriptionType) {
        this.subscriptionType = subscriptionType;
    }

    /**
     * Sets the Ack timeout.
     *
     * @param ackTimeout Ack timeout.
     */
    public void setAckTimeout(long ackTimeout) {
        this.ackTimeout = ackTimeout;
    }

    /**
     * Sets the Operation timeout.
     *
     * @param operationTimeout Ack timeout.
     */
    public void setOperationTimeout(int operationTimeout) {
        this.operationTimeout = operationTimeout;
    }

    //
//    /**
//     * Sets the threads.
//     *
//     * @param threads Number of threads.
//     */
//    public void setThreads(int threads) {
//        this.threads = threads;
//    }
//

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() throws IOException {
        A.notNull(getStreamer(), "streamer");
        A.notNull(getIgnite(), "ignite");
        A.notNull(topic, "topic");
        A.notNull(subscription, "subscription");
        A.notNull(serviceUrl, "serviceUrl");

//        A.notNull(consumerCfg, "kafka consumer config");
//        A.ensure(threads > 0, "threads > 0");
//        A.ensure(null != getSingleTupleExtractor() || null != getMultipleTupleExtractor(),
//            "Extractor must be configured");

        log = getIgnite().log();

        // TODO: パラメータ化 topicと同じ　nullチェックする
        //TODO: too many setting....
        // TODO: authenticationをMapパラメータにして渡す。テストかけない
        client = PulsarClient.builder()
            .serviceUrl(serviceUrl)
            .operationTimeout(10, TimeUnit.SECONDS)
            .build();

        consumer = client.newConsumer()
            .topic(topic)
            .subscriptionName(subscription)
            .ackTimeout(ackTimeout, TimeUnit.SECONDS)
            .subscriptionType(SubscriptionType.Exclusive)
            .subscribe();

        // Now launch all the consumer threads.
        executor = Executors.newFixedThreadPool(2);

        //TODO: 複数スレッド対応
        executor.execute(new Runnable() {
            @Override public void run() {
                do {
                    CompletableFuture<Message> fut = consumer.receiveAsync();

                    try {
                        fut.thenAccept(message -> {
                            addMessage(message);
                            try {
                                consumer.acknowledge(message);
                            }
                            catch (PulsarClientException e) {
                                //TODO: retry or retryしてもだめだったら強制終了
                                e.printStackTrace();
                            }
                        });
                    }
                    catch (Exception e) {
                        U.error(log, "Message is ignored due to an error", e);
                    }
                }
                while (!stopped);
            }
        });

    }

    /**
     * Stops streamer.
     */
    public void stop() throws PulsarClientException {
        stopped = true;

        if (consumer != null)
            consumer.close();
        // TODO: clientはcloseが必要かcheck
        if (client != null) {
            client.shutdown();
        }
    }
}
