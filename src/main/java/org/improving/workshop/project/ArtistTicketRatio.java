package org.improving.workshop.project;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.LinkedHashMap;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

/**
 * Class for solution to Question 1
 */
@Slf4j
public class ArtistTicketRatio {
    public static final String OUTPUT_TOPIC = "kafka-artist-ticket-ratio";
    public static final JsonSerde<StreamsPerArtist> STREAMS_PER_ARTIST_JSON_SERDE = new JsonSerde<>(StreamsPerArtist.class);
    public static final JsonSerde<TicketsPerArtist> TICKETS_PER_ARTIST_JSON_SERDE = new JsonSerde<>(TicketsPerArtist.class);
    public static final JsonSerde<ArtistRatio> ARTIST_RATIO_JSON_SERDE = new JsonSerde<>(ArtistRatio.class);

    /**
      Inital Start
      Kyle
    */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static KTable<String, TicketsPerArtist> getArtistTicketTable(final StreamsBuilder builder) {
        KTable<String, Event> eventsTable = builder
                .table(
                        TOPIC_DATA_DEMO_EVENTS,
                        Materialized
                                .<String, Event>as(persistentKeyValueStore("events"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_EVENT_JSON)
                );

        return builder
                .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .selectKey((ticketId, ticket) -> ticket.eventid(), Named.as("rekey-by-eventid"))
                .join(
                        eventsTable,
                        (eventId, ticket, event) -> new EventTicket(ticket, event)
                )
                .groupBy((k, v) -> v.event.artistid())
                .aggregate(
                        TicketsPerArtist::new,
                        (artistId, stream, artistTicketsCounts) -> {
                            artistTicketsCounts.addTicketIncrement(stream.event.artistid());
                            return artistTicketsCounts;
                        },
                        Materialized
                                .<String, TicketsPerArtist>as(persistentKeyValueStore("tickets-per-artist-counts"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(TICKETS_PER_ARTIST_JSON_SERDE)
                );
    }

    static void configureTopology(final StreamsBuilder builder) {
        KTable<String, TicketsPerArtist> ticketsPerArtistTable = getArtistTicketTable(builder);
        KTable<String, Artist> artistsTable = builder
                .table(
                        TOPIC_DATA_DEMO_ARTISTS,
                        Materialized
                                .<String, Artist>as(persistentKeyValueStore("artists"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_ARTIST_JSON)
                );

        builder
                .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .groupBy((k, v) -> v.artistid())
                .aggregate(
                        StreamsPerArtist::new,

                        (artistId, stream, artistStreamCounts) -> {
                            artistStreamCounts.addStreamIncrement(stream.artistid());
                            return artistStreamCounts;
                        },

                        Materialized
                                .<String, StreamsPerArtist>as(persistentKeyValueStore("stream-per-artist-counts"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(STREAMS_PER_ARTIST_JSON_SERDE)
                ).toStream()
                .join(ticketsPerArtistTable, (artistId, ticketsPerArtist, streamsPerArtist) -> new ArtistMetrics(streamsPerArtist, ticketsPerArtist))
                .peek((artistId, artistMetrics) -> log.info("Ticket Requested: {}", artistMetrics))
                .groupByKey()
                .aggregate(
                        ArtistRatio::new,
                        (artistId, artistMetrics, artistRatioCounter) -> {
                            artistRatioCounter.setRatio(artistMetrics.ticketsPerArtist.getMap().get(artistId), artistMetrics.streamsPerArtist.getMap().get(artistId));
                            return artistRatioCounter;
                        },
                        Materialized
                                .<String, ArtistRatio>as(persistentKeyValueStore("stream-per-artist-counts"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(ARTIST_RATIO_JSON_SERDE))
                .toStream()
                .join(artistsTable, (artistId, artistRatio, artist) -> new ArtistNameRatio(artistRatio, artist));
    }

    @Data
    @AllArgsConstructor
    public static class ArtistNameRatio {
        private ArtistRatio artistRatio;
        private Artist artist;
    }

    @Data
    @AllArgsConstructor
    public static class ArtistRatio {
        String artistId;
        double ratio;
        public ArtistRatio() {
        }
        public void setRatio(int ticketCount, int streamCount) {
            ratio = (double) ticketCount /streamCount;
        }
    }
    @Data
    @AllArgsConstructor
    public static class ArtistMetrics {
        private TicketsPerArtist ticketsPerArtist;
        private StreamsPerArtist streamsPerArtist;
    }

    @Data
    @AllArgsConstructor
    public static class EventTicket {
        private Ticket ticket;
        private Event event;
    }

    @Data
    @AllArgsConstructor
    public static class StreamsPerArtist {
        private HashMap<String, Integer> map;

        public StreamsPerArtist() {
            this.map = new HashMap<>();
        }

        public void addStreamIncrement(String id) {
            int value = map.get(id) + 1;
            map.put(id, value);
        }
    }
    @Data
    @AllArgsConstructor
    public static class TicketsPerArtist {
        private HashMap<String, Integer> map;

        public TicketsPerArtist() {
            this.map = new LinkedHashMap<>();
        }

        public void addTicketIncrement(String id) {
            int value = map.get(id) + 1;
            map.put(id, value);

        }
    }
}
