package org.improving.workshop.phase3;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.samples.PurchaseEventTicket;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.slf4j.event.KeyValuePair;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashSet;
import java.util.Set;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class GenreStatistics {

    public static final String OUTPUT_TOPIC = "kafka-genre-statistics";
    public static final JsonSerde<ArtistTicket> ARTIST_TICKET_JSON_SERDE = new JsonSerde<>(ArtistTicket.class);
    public static final JsonSerde<ArtistIdTicket> ARTIST_ID_TICKET_JSON_SERDE = new JsonSerde<>(ArtistIdTicket.class);
    public static final JsonSerde<GenreAggregate> GENRE_AGGREGATE_JSON_SERDE = new JsonSerde<>(GenreAggregate.class);
    public static final JsonSerde<GenreStatistic> GENRE_STATISTIC_JSON_SERDE = new JsonSerde<>(GenreStatistic.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    public static void configureTopology(final StreamsBuilder builder) {
        // store event in a table so that the stream can reference them to find event
        KTable<String, Event> eventTable = builder
                .table(
                        TOPIC_DATA_DEMO_EVENTS,
                        Materialized
                                .<String, Event>as(persistentKeyValueStore("customer"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_EVENT_JSON)
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

        eventTable.toStream().peek((key, event) -> log.info("Event '{}' registered with id {}.", key, event.id()));

        builder.stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .peek((ticketId, ticket) -> log.info("Ticket Received: {}", ticket))
                .selectKey((ticketId, ticket) -> ticket.eventid())
                .join(
                        eventTable,
                        (eventId, ticket, event) -> new ArtistIdTicket(ticket.eventid(), event.artistid(), ticket.price()),
                        Joined.with(Serdes.String(), SERDE_TICKET_JSON, SERDE_EVENT_JSON)
                )
                .selectKey((ticketId, artistIdTicket) -> artistIdTicket.artistId)
                .join(
                        artistTable,
                        (artistId, artistIdTicket, artist) -> new ArtistTicket(artistIdTicket.eventId, artist, artistIdTicket.price),
                        Joined.with(Serdes.String(), ARTIST_ID_TICKET_JSON_SERDE, SERDE_ARTIST_JSON)
                )
                .selectKey((artistId, artistTicket) -> artistTicket.artist.genre())
                .groupByKey(Grouped.with(Serdes.String(), ARTIST_TICKET_JSON_SERDE))
                .aggregate(
                        GenreAggregate::new,
                        (genre, artistTicket, aggregate) -> {
                            if (!aggregate.isInitialized) {
                                aggregate.initialize();
                            }
                            aggregate.addTicketSold(artistTicket);
                            return aggregate;
                        },
                        // ktable (materialized) configuration
                        Materialized
                                .<String, GenreAggregate>as(persistentKeyValueStore("artist-aggregate-table"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(GENRE_AGGREGATE_JSON_SERDE)
                )
                .toStream()
                .mapValues((key, value) -> new GenreStatistic(key, value.totalArtists, value.totalTicketsSold, value.totalRevenue))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), GENRE_STATISTIC_JSON_SERDE));
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ArtistIdTicket {
        private String eventId;
        private String artistId;
        private double price;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ArtistTicket {
        private String eventId;
        private Artist artist;
        private double price;
    }

    @Data
    @AllArgsConstructor
    public static class GenreAggregate {
        private boolean isInitialized;
        private long totalArtists;
        private Set<Artist> artists;
        private long totalTicketsSold;
        private double totalRevenue;

        public GenreAggregate() {
            this.isInitialized = false;
        }

        public void initialize() {
            isInitialized = true;
            totalArtists = 0L;
            artists = new HashSet<>();
            totalTicketsSold = 0L;
            totalRevenue = 0;
        }

        public void addTicketSold(ArtistTicket artistTicket) {
            if (!artists.contains(artistTicket.artist)) {
                artists.add(artistTicket.artist);
                this.totalArtists++;
            }
            this.totalTicketsSold++;
            this.totalRevenue += artistTicket.price;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class GenreStatistic {
        private String genre;
        private long totalArtists;
        private long totalTicketsSold;
        private double totalRevenue;
    }
}
