package org.improving.workshop.samples;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.stream.Stream;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class ArtistStreamByStateMetrics {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-artist-stream-by-state-metrics";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final JsonSerde<CustomerStream> CUSTOMER_STREAM_JSON_SERDE = new JsonSerde<>(CustomerStream.class);
    public static final JsonSerde<ArtistStateMetrics> ARTIST_STATE_METRICS_JSON_SERDE = new JsonSerde<>(ArtistStateMetrics.class);

    /**
     * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
     */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        // store addresses in a table so that the ticket can reference them to find capacity
        KTable<String, Address> addressesTable = builder
                .stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON))
                .selectKey((addressid, address) -> address.customerid())
                .toTable(Materialized
                            .<String, Address>as(persistentKeyValueStore("customer-addresses"))
                            .withKeySerde(Serdes.String())
                            .withValueSerde(SERDE_ADDRESS_JSON));

        // store addresses in a table so that the ticket can reference them to find capacity
        GlobalKTable<String, Customer> customersTable = builder
                .globalTable(
                        TOPIC_DATA_DEMO_CUSTOMERS,
                        Materialized
                                .<String, Customer>as(persistentKeyValueStore("customers"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_CUSTOMER_JSON)
                );

        // **********                                                 **********
        // ***** BQ1 - Which artist by name is streamed by the most CT residents
        // **********                                                 **********

        builder
                .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .peek((streamId, stream) -> log.info("Stream Received: {}", stream))

                // global table join (just for fun -- we end up rekeying by customer id below anyways)
                .join(
                        // global table to join against
                        customersTable,
                        // key selector
                        (streamId, stream) -> stream.customerid(),
                        // joiner
                        (stream, customer) -> new CustomerStream(stream, customer, null),
                        // named (for topology debugging)
                        Named.as("join-customerid-to-customer")
                )
                .selectKey((streamId, customerStream) -> customerStream.customer.id())
                // table join
                .join(
                        // table to join against
                        addressesTable,
                        // joiner
                        (customerStream, address) -> {
                            customerStream.setAddress(address);
                            return customerStream;
                        },
                        Joined.valueSerde(CUSTOMER_STREAM_JSON_SERDE)
                )
                .groupBy((streamId, customerStream) -> customerStream.getAddress().state(), Grouped.valueSerde(CUSTOMER_STREAM_JSON_SERDE))
                .aggregate(
                        // initializer (doesn't have key, value supplied so the actual initialization is in the aggregator)
                        ArtistStateMetrics::new,

                        // aggregator
                        (state, streamAddress, artistStateMetrics) -> {
                            // update the artist's metrics
                            artistStateMetrics.recordStream(streamAddress.stream.artistid());

                            return artistStateMetrics;
                        },

                        // ktable (materialized) configuration
                        Materialized
                                .<String, ArtistStateMetrics>as(persistentKeyValueStore("global-metrics-table"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(ARTIST_STATE_METRICS_JSON_SERDE)
                )
                .toStream()
                .peek(ArtistStreamByStateMetrics::logMetricsPretty)
                .to(OUTPUT_TOPIC, Produced.valueSerde(ARTIST_STATE_METRICS_JSON_SERDE));
    }

    @SneakyThrows
    private static void logMetricsPretty(String k, ArtistStateMetrics v) {
        log.info("{} Metrics Updated: {}", k, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(v));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomerStream {
        private Stream stream;
        private Customer customer;
        private Address address;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ArtistStateMetrics {
        // key: state, value: <key: artistid, value: streamcount>
        Map<String, Long> metrics;

        public void recordStream(String artistId) {
            // initialize metrics
            if (this.metrics == null) {
                this.metrics = new HashMap<>();
            }

            // Increment the artist's stream count
            if (this.metrics.containsKey(artistId)) {
                // If artistId already exists in the map, increment the stream count by 1
                this.metrics.put(artistId, this.metrics.get(artistId) + 1);
            } else {
                // If artistId doesn't exist in the map, initialize it with a stream count of 1
                this.metrics.put(artistId, 1L);
            }
        }
    }
}