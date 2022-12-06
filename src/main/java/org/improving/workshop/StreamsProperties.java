package org.improving.workshop;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class StreamsProperties {

    public static Properties build() {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-workshop-example-2");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "kafka-workshop-example-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-5d78q.us-east-2.aws.confluent.cloud:9092");

        // How to connect securely to the broker(s)
        streamsConfiguration.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        streamsConfiguration.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='MZ4PZ5TT5PQMWEYW' password='Sxg+3NcUsHndFXCLuemgiW/rhSMSKCUnxT+TDSfmvYsKrD6u5IIwui4uXPmHix8B';");
        streamsConfiguration.put("sasl.mechanism", "PLAIN");

        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonSerde");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches.
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Use a temporary directory for storing state, which will be automatically removed after the test.
        // streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        return streamsConfiguration;
    }
}
