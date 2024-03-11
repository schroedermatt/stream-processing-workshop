package org.improving.workshop.phase3;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.slf4j.event.KeyValuePair;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.*;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class TopCustomersWithMostUniqueArtists {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-top-3-customer-with-most-unique-artists";
    public static final JsonSerde<CustomerIdArtist> CUSTOMER_ID_ARTIST_STREAM_JSON_SERDE = new JsonSerde<>(CustomerIdArtist.class);
    public static final JsonSerde<CustomerArtist> CUSTOMER_ARTIST_STREAM_JSON_SERDE = new JsonSerde<>(CustomerArtist.class);
    public static final JsonSerde<SortedCounterMap> SORTED_COUNTER_MAP_JSON_SERDE = new JsonSerde<>(SortedCounterMap.class);
    public static final JsonSerde<CustomerUniqueArtistsList> CUSTOMER_UNIQUE_ARTISTS_LIST_JSON_SERDE = new JsonSerde<>(CustomerUniqueArtistsList.class);

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

    public static void configureTopology(final StreamsBuilder builder) {
        // store customer in a table so that the stream can reference them to find customer
        KTable<String, Customer> customerTable = builder
                .table(
                        TOPIC_DATA_DEMO_CUSTOMERS,
                        Materialized
                                .<String, Customer>as(persistentKeyValueStore("customer"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_CUSTOMER_JSON)
                );

        // store artist in a table so that the stream can reference them to find artist
        KTable<String, Artist> artistTable = builder
                .table(
                        TOPIC_DATA_DEMO_ARTISTS,
                        Materialized
                                .<String, Artist>as(persistentKeyValueStore("artist"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ARTIST_JSON)
                );

        // capture the backside of the table to log a confirmation that the Event was received
        customerTable.toStream().peek((key, customer) -> log.info("Customer '{}' registered with full name {}.", key, customer.fullname()));
        artistTable.toStream().peek((key, artist) -> log.info("Artist '{}' registered with name {}.", key, artist.name()));

        builder.stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .peek((streamId, stream) -> log.info("Stream Received: {}", stream))
                // Enrich Streams with Artist and Customer Data
                .selectKey((streamId, stream) -> stream.artistid())
                // Join Artists
                .join(
                        artistTable,
                        (artistId, stream, artist) -> new CustomerIdArtist(stream.customerid(), artist),
                        Joined.with(Serdes.String(), SERDE_STREAM_JSON, SERDE_ARTIST_JSON))
                .selectKey((streamId, customerIdArtist) -> customerIdArtist.getCustomerId())
                // Join Customers
                .join(
                        customerTable,
                        (customerId, customerIdArtist, customer) -> new CustomerArtist(customerIdArtist.getCustomerId(), customer, customerIdArtist.getArtist()),
                        Joined.with(Serdes.String(), CUSTOMER_ID_ARTIST_STREAM_JSON_SERDE, SERDE_CUSTOMER_JSON)
                )
                .peek((streamId, customerArtist) -> log.info("Stream: {} with CustomerArtist: {}", streamId, customerArtist))
                // Re-partition by customerId to create a KGroupedStream of Key customerId and CustomerArtist
                .selectKey((key, value) -> "global")
                .groupByKey(Grouped.with(Serdes.String(), CUSTOMER_ARTIST_STREAM_JSON_SERDE))
                // Aggregate each customer's unique artists using an Aggregate
                .aggregate(SortedCounterMap::new, customerUniqueArtistAggregator(), customerUniqueArtistMaterializedView())
                .toStream()
                .mapValues(value -> value.top(3))
                // Get the top 3 customers with the highest unique artists count.
                .peek((globalKey, top3CustomersWithUniqueArtists) -> log.info("Top 3 Customers with most unique artists: {}", top3CustomersWithUniqueArtists))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), CUSTOMER_UNIQUE_ARTISTS_LIST_JSON_SERDE));
    }

    private static Aggregator<String, CustomerArtist, SortedCounterMap> customerUniqueArtistAggregator() {
        return (globalKey, customerArtist, sortedCounterMap) -> {
            sortedCounterMap.addUniqueArtistForCustomer(customerArtist);
            return sortedCounterMap;
        };
    }

    private static Materialized<String, SortedCounterMap, KeyValueStore<Bytes, byte[]>> customerUniqueArtistMaterializedView() {
        // kTable (materialized) configuration
        return Materialized
                .<String, SortedCounterMap>as(persistentKeyValueStore("unique-stream-with-artist-table"))
                .withKeySerde(Serdes.String())
                .withValueSerde(SORTED_COUNTER_MAP_JSON_SERDE);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerIdArtist {
        private String customerId;
        private Artist artist;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerArtist {
        private String customerId;
        private Customer customer;
        private Artist artist;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerUniqueArtists {
        private Customer customer;
        private UniqueArtistsSet uniqueArtistsSet;
        private Long uniqueCount;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerUniqueArtistsList {
        private List<CustomerUniqueArtists> customerUniqueArtistsList;
    }

    @Data
    @AllArgsConstructor
    public static class UniqueArtistsSet {
        private Set<Artist> uniqueArtists;

        public UniqueArtistsSet() {
            uniqueArtists = new HashSet<>();
        }
    }

    @Data
    @AllArgsConstructor
    public static class SortedCounterMap {
        private int maxSize;
        private LinkedHashMap<String, CustomerUniqueArtists> map;

        public SortedCounterMap(int maxSize) {
            this.maxSize = maxSize;
            this.map = new LinkedHashMap<>();
        }

        public SortedCounterMap() { this(1000000); }

        public void addUniqueArtistForCustomer(CustomerArtist customerArtist) {
            CustomerUniqueArtists customerUniqueArtists = map.computeIfAbsent(customerArtist.customer.id(), value -> new CustomerUniqueArtists(customerArtist.customer, new UniqueArtistsSet(), 0L));
            customerUniqueArtists.uniqueArtistsSet.uniqueArtists.add(customerArtist.artist);
            customerUniqueArtists.uniqueCount = (long) customerUniqueArtists.uniqueArtistsSet.uniqueArtists.size();

            // Sort the map.
            this.map = map.entrySet().stream()
                    .sorted(reverseOrder(Map.Entry.comparingByValue(Comparator.comparing(CustomerUniqueArtists::getUniqueCount))))
                    .limit(maxSize)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        public CustomerUniqueArtistsList top(int limit) {
            return new CustomerUniqueArtistsList(this.map.entrySet().stream()
                    .limit(limit)
                    .map(Map.Entry::getValue)
                    .toList());
        }
    }
}
