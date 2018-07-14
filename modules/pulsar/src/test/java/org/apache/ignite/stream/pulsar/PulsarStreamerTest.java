package org.apache.ignite.stream.pulsar;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Tests {@link PulsarStreamer}.
 */
public class PulsarStreamerTest extends GridCommonAbstractTest {

    /** Count. */
    private static final int CNT = 1;
    /** Test topic. */
    private static final String TOPIC_NAME = "test_topic";
    /** Test subscription. */
    private static final String SUBSCRIPTION_NAME = "my-subscription";
    /** ::TODO keyの設定をみなおす */
    private static final String TEST_KEY = "TEST_KEY";
    /** ::TODO embeded化 */
    private PulsarClient pulsarClient;

    /** Constructor. */
    public PulsarStreamerTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        grid().<Integer, String>getOrCreateCache(defaultCacheConfiguration());
        pulsarClient = PulsarClient.builder()
            .serviceUrl("pulsar://localhost:6650")
            .build();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid().cache(DEFAULT_CACHE_NAME).clear();

    }

    /**
     * Sends messages to Pulsar.
     *
     * @return Map of key value messages.
     */
    private Map<String, String> produceStream() throws PulsarClientException {
        Map<String, String> keyValMap = new HashMap<>();
        String key = new String(TEST_KEY);
        String val = new String("test message");

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(TOPIC_NAME)
            .create();
        // You can then send messages to the broker and topic you specified:
        producer.send("test message".getBytes());

        keyValMap.put(key, val);
        return keyValMap;
    }

    /**
     * Tests Pulsar streamer.
     */
    public void testStart() throws IOException, InterruptedException {

        Ignite ignite = grid();

        PulsarStreamer pulsarStreamer = new PulsarStreamer();

        // Get the cache.
        IgniteCache<String, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

        // Set Ignite instance.
        pulsarStreamer.setIgnite(ignite);

        // send pulsar
        Map<String, String> keyValMap = produceStream();

        Map<String, String> entries = new HashMap<>();

        try (IgniteDataStreamer<String, String> stmr = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            stmr.allowOverwrite(true);
            stmr.autoFlushFrequency(10);

            pulsarStreamer.setServiceUrl("pulsar://localhost:6650");
            pulsarStreamer.setStreamer(stmr);
            pulsarStreamer.setTopic(TOPIC_NAME);
            pulsarStreamer.setSubscription(SUBSCRIPTION_NAME);

            pulsarStreamer.setMultipleTupleExtractor(
                new StreamMultipleTupleExtractor<org.apache.pulsar.client.api.Message, String, String>() {
                    @Override public Map<String, String> extract(org.apache.pulsar.client.api.Message msg) {

                        try {
                            String key = TEST_KEY;
                            String val = new String(msg.getData());

                            // Convert the message into number of cache entries with same key or dynamic key from actual message.
                            // For now using key as cache entry key and value as cache entry value - for test purpose.
                            entries.put(key, val);
                        }
                        catch (Exception ex) {
                            fail("Unexpected error." + ex);
                        }

                        return entries;
                    }
                });

            // start
            pulsarStreamer.start();

            final CountDownLatch latch = new CountDownLatch(CNT);

            IgniteBiPredicate<UUID, CacheEvent> locLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
                @Override public boolean apply(UUID uuid, CacheEvent evt) {
                    latch.countDown();

                    return true;
                }
            };

            ignite.events(ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME)).remoteListen(locLsnr, null, EVT_CACHE_OBJECT_PUT);

            // Checks all events successfully processed in 10 seconds.
            assertTrue(latch.await(10, TimeUnit.SECONDS));

            for (Map.Entry<String, String> entry : keyValMap.entrySet()) {
                assertEquals(entry.getValue(), cache.get(TEST_KEY));
            }

        }
        catch (PulsarClientException e) {
            e.printStackTrace();
        }
        finally {
            if (pulsarStreamer != null)
                pulsarStreamer.stop();
        }
    }
}
