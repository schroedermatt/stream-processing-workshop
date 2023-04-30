package org.improving.workshop.project;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
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
    public static final JsonSerde<StreamsPerArtist2> STREAMS_PER_ARTIST2_JSON_SERDE = new JsonSerde<>(StreamsPerArtist2.class);
    public static final JsonSerde<TicketsPerArtist> TICKETS_PER_ARTIST_JSON_SERDE = new JsonSerde<>(TicketsPerArtist.class);
    public static final JsonSerde<TicketsPerArtist2> TICKETS_PER_ARTIST2_JSON_SERDE = new JsonSerde<>(TicketsPerArtist2.class);
    public static final JsonSerde<ArtistRatio> ARTIST_RATIO_JSON_SERDE = new JsonSerde<>(ArtistRatio.class);
    public static final JsonSerde<ArtistMetrics> ARTIST_METRICS_JSON_SERDE = new JsonSerde<>(ArtistMetrics.class);
    public static final JsonSerde<EventTicket> EVENT_TICKET_JSON_SERDE = new JsonSerde<>(EventTicket.class);
    public static final JsonSerde<ArtistNameRatio> ARTIST_NAME_RATIO_JSON_SERDE = new JsonSerde<>(ArtistNameRatio.class);

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

    static KTable<String, Artist> getArtistTable(final StreamsBuilder builder) {
        return builder
                .table(
                        TOPIC_DATA_DEMO_ARTISTS,
                        Materialized
                                .<String, Artist>as(persistentKeyValueStore("artists"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_ARTIST_JSON)
                );

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
                .groupBy((k, v) -> v.event.artistid(), Grouped.with(Serdes.String(), EVENT_TICKET_JSON_SERDE))
                .aggregate(
                        TicketsPerArtist::new,
                        //TicketsPerArtist2::new,
                        (artistId, eventTicket, artistTicketsCounts) -> {
                            //artistTicketsCounts.addTicketIncrement();
                            artistTicketsCounts.addTicketIncrement(eventTicket.event.artistid());
                            return artistTicketsCounts;
                        },
                        Materialized
                                .<String, TicketsPerArtist>as(persistentKeyValueStore("tickets-per-artist-counts"))
                                //.<String, TicketsPerArtist2>as(persistentKeyValueStore("tickets-per-artist-counts2"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(TICKETS_PER_ARTIST_JSON_SERDE)
                                //.withValueSerde(TICKETS_PER_ARTIST2_JSON_SERDE)
                );
    }

    static void configureTopology(final StreamsBuilder builder) {
        KTable<String, TicketsPerArtist> ticketsPerArtistTable = getArtistTicketTable(builder);


        KTable<String, Artist> artistsTable = getArtistTable(builder);
        artistsTable.toStream().peek((eventId, eventStatus) -> log.info("WOWOWOWOWOWO '{}'", eventStatus));


        KTable<String, StreamsPerArtist> streamsPerArtistTable = builder.stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .groupBy((k, v) -> v.artistid(), Grouped.with(Serdes.String(), SERDE_STREAM_JSON))
                .aggregate(
                        StreamsPerArtist::new,
                        //StreamsPerArtist2::new,
                        (artistId, stream, artistStreamCounts) -> {
                            artistStreamCounts.addStreamIncrement(stream.artistid());
                            //artistStreamCounts.addStreamIncrement();
                            return artistStreamCounts;
                        },

                        Materialized
                                .<String, StreamsPerArtist>as(persistentKeyValueStore("stream-per-artist-counts"))
                                //.<String, StreamsPerArtist2>as(persistentKeyValueStore("stream-per-artist-counts"))
                                .withKeySerde(Serdes.String())
                                //.withValueSerde(STREAMS_PER_ARTIST2_JSON_SERDE)
                                .withValueSerde(STREAMS_PER_ARTIST_JSON_SERDE)
                );

        KTable<String, ArtistRatio> artistRatioTable = streamsPerArtistTable.toStream()
                .join(ticketsPerArtistTable, (artistId, ticketsPerArtist, streamsPerArtist) -> new ArtistMetrics(streamsPerArtist, ticketsPerArtist))
                .groupByKey(Grouped.with(Serdes.String(), ARTIST_METRICS_JSON_SERDE))

                .aggregate(
                        ArtistRatio::new,
                        (artistId, artistMetrics, artistRatioCounter) -> {
                            artistRatioCounter.setRatio(artistMetrics.ticketsPerArtist.getMap().get(artistId), artistMetrics.streamsPerArtist.getMap().get(artistId), artistId);
                            //artistRatioCounter.setRatio(artistMetrics.getTicketsPerArtist().count, artistMetrics.streamsPerArtist.getCount());
                            return artistRatioCounter;
                        },
                        Materialized
                                .<String, ArtistRatio>as(persistentKeyValueStore("artist-ratio"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(ARTIST_RATIO_JSON_SERDE));

        artistRatioTable.toStream()
                .peek((eventId, eventStatus) -> log.info("WOWOWOWOWOWO '{}'", eventStatus))
                .join(artistsTable, (artistId, artistRatio, artist) -> new ArtistNameRatio(artistRatio, artist))
                .peek((eventId, eventStatus) -> log.info("WOWOWOWOWOWO '{}'", eventStatus))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), ARTIST_NAME_RATIO_JSON_SERDE));
    }

    @Data
    @AllArgsConstructor
    public static class ArtistNameRatio {
        private ArtistRatio artistRatio;
        private Artist artist;
        public ArtistNameRatio() {
        }
    }

    @Data
    @AllArgsConstructor
    public static class ArtistRatio {
        String artistId;
        double ratio;
        public ArtistRatio() {
        }
        public void setRatio(int ticketCount, int streamCount, String Id) {
            artistId = Id;
            ratio = (double) ticketCount /streamCount;
        }
    }
    @Data
    @AllArgsConstructor
    public static class ArtistMetrics {
        private TicketsPerArtist ticketsPerArtist;
        //private TicketsPerArtist2 ticketsPerArtist;
        private StreamsPerArtist streamsPerArtist;
        //private StreamsPerArtist2 streamsPerArtist;
    }

    @Data
    @AllArgsConstructor
    public static class EventTicket {
        private Ticket ticket;
        private Event event;
        public EventTicket() {}
    }

    @Data
    @AllArgsConstructor
    public static class StreamsPerArtist {
        private HashMap<String, Integer> map;

        public StreamsPerArtist() {
            this.map = new HashMap<>();
        }

        public void addStreamIncrement(String id) {
            map.merge(id, 1, Integer::sum);
        }
    }

    @Data
    @AllArgsConstructor
    public static class StreamsPerArtist2 {
        private Artist artist;
        private int count;

        public StreamsPerArtist2() {

        }

        public void addStreamIncrement() {
            count++;
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
            map.merge(id, 1, Integer::sum);
        }
    }

    @Data
    @AllArgsConstructor
    public static class TicketsPerArtist2 {
        private Artist artist;
        private int count;

        public TicketsPerArtist2() {

        }

        public void addTicketIncrement() {
            count++;
        }
    }
}
