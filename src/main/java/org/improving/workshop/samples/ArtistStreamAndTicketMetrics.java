package org.improving.workshop.samples;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class ArtistStreamAndTicketMetrics {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC_GLOBAL_METRICS = "kafka-workshop-matt-artist-stream-and-ticket-metrics";
    public static final String OUTPUT_TOPIC_TOP_GROSSING = "kafka-workshop-matt-artist-top-grossing-event";
    public static final String OUTPUT_TOPIC_5K_STREAMER = "kafka-workshop-matt-artist-with-5k-streams-and-low-event-attendance";
    public static final String OUTPUT_TOPIC_LOWEST_PERFORMING_VENUE = "kafka-workshop-matt-artist-lowest-performing-venue";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final JsonSerde<ArtistMetrics> METRICS_JSON_SERDE = new JsonSerde<>(ArtistMetrics.class);
    public static final JsonSerde<EventTicket> EVENT_TICKET_JSON_SERDE = new JsonSerde<>(EventTicket.class);
    public static final JsonSerde<GlobalMetrics> GLOBAL_METRICS_JSON_SERDE = new JsonSerde<>(GlobalMetrics.class);

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
        // **********                             **********
        // ***** COMMON TOPOLOGY COMPONENTS
        // **********                             **********

        // store artists in a table to be referenced to fill in full artist details
        // key: artistid (String), value: Artist
        KTable<String, Artist> artistsTable = buildArtistsTable(builder);

        // store venues in a table to be referenced to fill in full venue details
        // key: venueid (String), value: Venue
        KTable<String, Venue> venuesTable = buildVenuesTable(builder);

        // aggregate artist ticket metrics
        // key: artistid (String), value: ArtistMetrics
        KTable<String, ArtistMetrics> artistTicketMetricsTable = buildArtistTicketMetricsTable(builder);

        // turn the table back into a stream to be used below
        KStream<String, ArtistMetrics> artistTicketMetricsStream = artistTicketMetricsTable.toStream();

        // count all artist streams
        // key: artistid (String), value: totalStreamCount (Long)
        KTable<String, Long> artistStreamCountTable = buildAristStreamMetricsTable(builder);

        // join the artist ticket metrics and stream count tables
        KStream<String, ArtistMetrics> artistTicketAndStreamingMetrics = outerJoinArtistTicketAndStreamMetrics(
                builder, artistTicketMetricsTable, artistStreamCountTable);

        // **********                             **********
        // ***** BQ1 - Find each artist's top grossing event
        // **********                             **********

        artistTicketMetricsStream
                // join the metrics to the artist to fill in artist details
                .join(
                        artistsTable,
                        (artistId, metrics, artist) -> {
                            // find the top grossing event
                            String topGrossingEventId = metrics.topGrossingEvent();
                            EventDetails topGrossingEvent = metrics.eventMetrics.get(topGrossingEventId);

                            // return a simple string message explaining the updated state
                            return String.format("'%s' (%s) event ('%s') has sold %f tickets and grossed $%f! This is their top grossing event.",
                                    artist.name(),
                                    artistId,
                                    topGrossingEventId,
                                    topGrossingEvent.ticketsSold,
                                    topGrossingEvent.grossEarnings);
                        },
                        Joined.with(Serdes.String(), METRICS_JSON_SERDE, SERDE_ARTIST_JSON)
                )
                .peek((k, v) -> log.info(v))
                .to(OUTPUT_TOPIC_TOP_GROSSING, Produced.with(Serdes.String(), Serdes.String()));

        // **********                                        **********
        // ***** BQ2 - Find artists with 5k streams and 1/2 sold events
        // **********                                        **********

        artistTicketAndStreamingMetrics
                .filter((artistId, artistMetrics) -> artistMetrics.totalStreamCount >= 500 && artistMetrics.eventTicketToCapacityRatio() < 0.5)
                // join the metrics to the artist to fill in artist details
                .join(
                        artistsTable,
                        (artistId, metrics, artist) -> String.format("'%s' (%s) has been streamed %f times but has sold %f%% of their event tickets.",
                                artist.name(),
                                artistId,
                                metrics.totalStreamCount,
                                metrics.eventTicketToCapacityRatio() * 100),
                        Joined.with(Serdes.String(), METRICS_JSON_SERDE, SERDE_ARTIST_JSON)
                )
                .peek((k, v) -> log.info(v))
                .to(OUTPUT_TOPIC_5K_STREAMER, Produced.with(Serdes.String(), Serdes.String()));

        // **********                                                       **********
        // ***** BQ3 - Find venue with the lowest proportion of tickets sold by artist
        // **********                                                       **********

        artistTicketMetricsStream
                // todo - join to artist/venue
                .mapValues((ArtistMetrics::lowestPerformingVenueId))
                .peek((k, v) -> log.info("Venue '{}' for artist '{}' is the worst.", v, k))
                .to(OUTPUT_TOPIC_LOWEST_PERFORMING_VENUE, Produced.with(Serdes.String(), Serdes.String()));

        // **********                                                           **********
        // ***** BQ1 - Find the 5 artists who have the highest ratio of tickets to streams
        // **********                                                           **********

        artistTicketAndStreamingMetrics
                .map((k,v) -> {
                    v.setArtistId(k);

                    // align all artist metrics to a single partition for the cross-artist analytics
                    return new KeyValue<>("STATIC_KEY", v);
                })
                .groupByKey(Grouped.with(Serdes.String(), METRICS_JSON_SERDE))
                .aggregate(
                        // initializer (doesn't have key, value supplied so the actual initialization is in the aggregator)
                        GlobalMetrics::new,

                        // aggregator
                        (staticKey, artistMetrics, globalMetrics) -> {
                            // update the artist's metrics
                            globalMetrics.setArtistTicketToStreamRatio(artistMetrics.artistId, artistMetrics.getTicketToStreamRatio());

                            return globalMetrics;
                        },

                        // ktable (materialized) configuration
                        Materialized
                                .<String, GlobalMetrics>as(persistentKeyValueStore("global-metrics-table"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(GLOBAL_METRICS_JSON_SERDE)
                )
                .toStream()
                // we don't need the key since it's "STATIC_KEY", so null it out
                .selectKey((k, v) -> null)
                // .peek(ArtistStreamAndTicketMetrics::logMetricsPretty)
                .to(OUTPUT_TOPIC_GLOBAL_METRICS, Produced.valueSerde(GLOBAL_METRICS_JSON_SERDE));
    }

    //************************************//
    //************************************//
    //         TOPOLOGY BUILDERS          //
    //************************************//
    //************************************//

    private static KTable<String, ArtistMetrics> buildArtistTicketMetricsTable(StreamsBuilder builder) {
        // store events in a table so that the ticket can reference them to find capacity
        // key: eventid (String), value: Event
        KTable<String, Event> eventsTable = buildEventsTable(builder);

        return builder
                .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .peek((ticketId, ticketRequest) -> log.info("Ticket Requested: {}", ticketRequest))

                // rekey by eventid so we can join against the event ktable
                .selectKey((ticketId, ticketRequest) -> ticketRequest.eventid(), Named.as("rekey-t-by-eventid"))

                // join the incoming ticket to the event that it is for
                .join(
                        eventsTable,
                        (eventId, ticket, event) -> new EventTicket(ticket, event),
                        Joined.with(Serdes.String(), SERDE_TICKET_JSON, SERDE_EVENT_JSON)
                )

                // rekey by artist id so we can aggregate on tickets by artistid
                .groupBy((eventId, eventTicket) -> eventTicket.getEvent().artistid(), Grouped.with(Serdes.String(), EVENT_TICKET_JSON_SERDE))
                .aggregate(
                        // initializer (doesn't have key, value supplied so the actual initialization is in the aggregator)
                        ArtistMetrics::new,

                        // aggregator
                        (artistId, eventTicket, metrics) -> {
                            // update the artist's ticket metrics
                            metrics.recordTicket(eventTicket);

                            return metrics;
                        },

                        // ktable (materialized) configuration
                        Materialized
                                .<String, ArtistMetrics>as(persistentKeyValueStore("artist-ticket-metrics-table"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(METRICS_JSON_SERDE)
                );
    }

    private static KTable<String, Long> buildAristStreamMetricsTable(StreamsBuilder builder) {
        return builder
                .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .peek((streamId, stream) -> log.info("Stream Received: {}", stream))

                // rekey by artist id so we can count all streams by artistid
                .groupBy((streamId, stream) -> stream.artistid())
                .count();
    }

    private static KStream<String, ArtistMetrics> outerJoinArtistTicketAndStreamMetrics(StreamsBuilder builder, KTable<String, ArtistMetrics> artistTicketMetricsTable, KTable<String, Long> artistStreamCountTable) {
        return artistTicketMetricsTable
                .outerJoin(artistStreamCountTable, (ticketMetrics, streamCount) -> {
                    // if streams come in before tickets, ticketMetrics will be null
                    if (ticketMetrics == null) {
                        return new ArtistMetrics(streamCount);
                    }

                    // if tickets come in first or both ticketMetrics and streamCount exist, update the ticketMetrics instance
                    ticketMetrics.setTotalStreamCount(streamCount);
                    return ticketMetrics;
                })
                .toStream();
    }

    private static KTable<String, Artist> buildArtistsTable(StreamsBuilder builder) {
        return builder
                .table(
                        TOPIC_DATA_DEMO_ARTISTS,
                        Materialized
                                .<String, Artist>as(persistentKeyValueStore("artists"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ARTIST_JSON)
                );
    }

    private static KTable<String, Event> buildEventsTable(StreamsBuilder builder) {
        return builder
                .table(
                        TOPIC_DATA_DEMO_EVENTS,
                        Materialized
                                .<String, Event>as(persistentKeyValueStore("events"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Streams.SERDE_EVENT_JSON)
                );
    }

    private static KTable<String, Venue> buildVenuesTable(StreamsBuilder builder) {
        return builder
                .table(
                        TOPIC_DATA_DEMO_VENUES,
                        Materialized
                                .<String, Venue>as(persistentKeyValueStore("venues"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_VENUE_JSON)
                );
    }

    //************************************//
    //************************************//
    //          TOPOLOGY MODELS           //
    //************************************//
    //************************************//

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class EventTicket {
        private Ticket ticket;
        private Event event;
    }

    @Data
    @AllArgsConstructor
    static class GlobalMetrics {
        // key: artistId, value: ticketToStreamRatio
        Map<String, Double> artistTicketToStreamRatio;
        public GlobalMetrics() {
            this.artistTicketToStreamRatio = new HashMap<>();
        }

        public void setArtistTicketToStreamRatio(String artistId, double ticketToStreamRatio) {
            this.artistTicketToStreamRatio.put(artistId, ticketToStreamRatio);

            // sort the map from highest to lowest
            this.artistTicketToStreamRatio = this.artistTicketToStreamRatio.entrySet()
                    .stream()
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .collect(
                            Collectors.toMap(
                                    Map.Entry::getKey,
                                    Map.Entry::getValue,
                                    (oldValue, newValue) -> oldValue,
                                    LinkedHashMap::new)
                    );
        }

//        public Map<String, Double> getTop5TicketToStreamRatioArtists() {
//            Map<String, Double> top5Ratios = new LinkedHashMap<>();
//            artistTicketToStreamRatio.entrySet()
//                    .stream()
//                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
//                    .limit(5)
//                    .forEachOrdered(entry -> top5Ratios.put(entry.getKey(), entry.getValue()));
//            return top5Ratios;
//        }
    }

    @Data
    @AllArgsConstructor
    static class ArtistMetrics {

        // you could store the whole artist in here too
        // private Artist artist;
        private String artistId;
        private double totalStreamCount;
        private Map<String, EventDetails> eventMetrics;

        public ArtistMetrics() {
            this.totalStreamCount = 0;
            this.eventMetrics = new HashMap<>();
        }

        public ArtistMetrics(Long totalStreamCount) {
            this.totalStreamCount = totalStreamCount;
            this.eventMetrics = new HashMap<>();
        }

        public void recordTicket(EventTicket eventTicket) {
            // if this is the first ticket sold for an event, initialize the event metrics
            if (!eventMetrics.containsKey(eventTicket.event.id())) {
                EventDetails eventDetails = new EventDetails();
                eventDetails.setVenueId(eventTicket.event.venueid());
                eventDetails.setCapacity(eventTicket.event.capacity());
                eventDetails.setEventdate(eventTicket.event.eventdate());
                eventDetails.recordTicket(eventTicket.ticket.price());
                eventMetrics.put(eventTicket.event.id(), eventDetails);
            } else {
                eventMetrics.get(eventTicket.event.id()).recordTicket(eventTicket.ticket.price());
            }
        }

        public void setTotalStreamCount(Long streamCount) {
            this.totalStreamCount = streamCount == null ? 0 : streamCount;
        }

        public double getTicketToStreamRatio() {
            if (this.totalStreamCount == 0) {
                return 0;
            }

            return this.totalTicketsSold() / this.totalStreamCount;
        }

        public double totalTicketsSold() {
            double total = 0;
            for (EventDetails eventDetails : eventMetrics.values()) {
                total += eventDetails.getTicketsSold();
            }
            return total;
        }

        public String topGrossingEvent() {
            String highestGrossingEventId = null;
            double highestGrossEarnings = 0;

            for (Map.Entry<String, EventDetails> entry : eventMetrics.entrySet()) {
                EventDetails eventDetails = entry.getValue();
                if (eventDetails.getGrossEarnings() > highestGrossEarnings) {
                    highestGrossingEventId = entry.getKey();
                    highestGrossEarnings = eventDetails.getGrossEarnings();
                }
            }

            return highestGrossingEventId;
        }

        public double eventTicketToCapacityRatio() {
            if (eventMetrics.isEmpty()) {
                return 0;
            }

            double totalRatio = 0;
            for (EventDetails eventDetails : eventMetrics.values()) {
                totalRatio += eventDetails.ticketToCapacityRatio();
            }
            return totalRatio / eventMetrics.size();
        }

        public String lowestPerformingVenueId() {
            String lowestVenueId = null;
            double lowestRatio = Double.MAX_VALUE;

            for (EventDetails eventDetails : eventMetrics.values()) {
                double ratio = eventDetails.ticketToCapacityRatio();
                if (ratio < lowestRatio) {
                    lowestRatio = ratio;
                    lowestVenueId = eventDetails.getVenueId();
                }
            }

            return lowestVenueId;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class EventDetails {
        private double grossEarnings = 0;
        private double ticketsSold = 0;
        private double capacity = 0;
        private String venueId;
        private String eventdate;

        public void recordTicket(double ticketPrice) {
            this.ticketsSold++;
            this.grossEarnings = this.grossEarnings + ticketPrice;
        }

        public double ticketToCapacityRatio() {
            return this.ticketsSold / this.capacity;
        }
    }

    //************************************//
    //************************************//
    //         TOPOLOGY UTILITIES         //
    //************************************//
    //************************************//

    @SneakyThrows
    private static void logMetricsPretty(Object k, GlobalMetrics v) {
        log.info("Global Metrics Updated: {}", MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(v));
    }
}