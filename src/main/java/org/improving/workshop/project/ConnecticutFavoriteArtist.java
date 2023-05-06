package org.improving.workshop.project;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.stream.Stream;
import org.springframework.kafka.support.serializer.JsonSerde;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

/**
 * Class for solution to Question 3
 */
@Slf4j
public class ConnecticutFavoriteArtist {
    public static final String CT_FAVORITE_ARTIST_TOPIC = "ct-favorite-artist-topic";
    public static final JsonSerde<ArtistStreams> ARTIST_STREAMS_JSON_SERDE = new JsonSerde<>(ArtistStreams.class);
    public static final JsonSerde<ArtistStreamsWithIsNewMaxFlag> ARTIST_STREAMS_IS_NEW_MAX_JSON_SERDE = new JsonSerde<>(ArtistStreamsWithIsNewMaxFlag.class);

    static void configureTopology(final StreamsBuilder builder) {
        KTable<String, Artist> artistsTable = builder
                .table(
                        TOPIC_DATA_DEMO_ARTISTS,
                        Materialized
                                .<String, Artist>as(persistentKeyValueStore("artists"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ARTIST_JSON)
                );
        KTable<String, Customer> customersTable = builder
                .table(
                        TOPIC_DATA_DEMO_CUSTOMERS,
                        Materialized
                                .<String, Customer>as(persistentKeyValueStore("customers"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_CUSTOMER_JSON)
                );

        KTable<String, CustomerWithAddress> ctAddressTable = builder.stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON))
                .filter((addressId, address) -> address.state().equalsIgnoreCase("ct"))
                .selectKey((addressId, address) -> address.customerid())
                .toTable()
                .join(customersTable, (ctAddress, customer) -> new CustomerWithAddress(customer, ctAddress));

        KStream<String, Stream> streamsStream = builder.stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON));

        KTable<String, Long> artistStreamsInCtCount = streamsStream
                .selectKey((streamId, stream) -> stream.customerid())
                .join(ctAddressTable, (stream, ctAddress) -> new ArtistStreams(stream.artistid(), 1))
                .selectKey((key, artistStreams) -> artistStreams.artistid)
                .groupByKey(Grouped.with(Serdes.String(), ARTIST_STREAMS_JSON_SERDE))
                .count();

        artistStreamsInCtCount
                .toStream()
                .mapValues(ArtistStreams::new)
                .groupBy((k, v) -> "CT", Grouped.with(Serdes.String(), ARTIST_STREAMS_JSON_SERDE))
                .aggregate(
                        () -> new ArtistStreamsWithIsNewMaxFlag(new ArtistStreams("", 0), false),
                        (state, artistStream, aggregate) -> {
                            if (artistStream.totalStreams > aggregate.artistStreams.totalStreams) {
                                aggregate.artistStreams = artistStream;
                                aggregate.isNewMax = true;
                            } else {
                                aggregate.isNewMax = false;
                            }

                            return aggregate;
                        },
                        Materialized.with(Serdes.String(), ARTIST_STREAMS_IS_NEW_MAX_JSON_SERDE)
                )
                .toStream()
                .filter((key, artistStreamsWithFlag) -> artistStreamsWithFlag.isNewMax)
                .mapValues((key, artistStreamsWithFlag) -> artistStreamsWithFlag.artistStreams)
                .to(CT_FAVORITE_ARTIST_TOPIC, Produced.with(Serdes.String(), ARTIST_STREAMS_JSON_SERDE));

    }

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ArtistStreams {
        String artistid;
        long totalStreams;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ArtistStreamsWithIsNewMaxFlag {
        ArtistStreams artistStreams;
        boolean isNewMax;
    }

    @Data
    @AllArgsConstructor
    public static class CustomerWithAddress {
        Customer customer;
        Address address;
    }
}

