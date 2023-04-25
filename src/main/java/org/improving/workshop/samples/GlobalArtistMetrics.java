package org.improving.workshop.samples;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class GlobalArtistMetrics {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-artist-metrics";

    public static final JsonSerde<Metrics> METRICS_JSON_SERDE = new JsonSerde<>(Metrics.class);
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
        // store events in a table so that the ticket can reference them to find capacity
        KTable<String, Event> eventsTable = builder
                .table(
                        TOPIC_DATA_DEMO_EVENTS,
                        Materialized
                            .<String, Event>as(persistentKeyValueStore("events"))
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Streams.SERDE_EVENT_JSON)
                );

        // count all artist tickets
        KTable<String, Long> artistTicketCountTable = builder
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

            // rekey by artist id so we can group/count all tickets by artistid
            .selectKey((s, eventTicket) -> eventTicket.getEvent().artistid(), Named.as("rekey-et-by-artistid"))
            .groupByKey(Grouped.with(Serdes.String(), EVENT_TICKET_JSON_SERDE))
            .count();

        // count all artist streams
        KTable<String, Long> artistStreamCountTable = builder
                .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .peek((streamId, stream) -> log.info("Stream Received: {}", stream))

                // rekey by artist id so we can group/count all streams by artistid
                .selectKey((streamId, stream) -> stream.artistid(), Named.as("rekey-s-by-artistid"))
                .groupByKey()
                .count();

        // join the artist ticket and stream count tables into a single metrics object
        artistTicketCountTable
                .outerJoin(artistStreamCountTable, Metrics::new)
                .toStream()
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
                            globalMetrics.setArtistMetrics(artistMetrics.artistId, artistMetrics);

                            return globalMetrics;
                        },

                        // ktable (materialized) configuration
                        Materialized
                                .<String, GlobalMetrics>as(persistentKeyValueStore("global-metrics-table"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(GLOBAL_METRICS_JSON_SERDE)
                )
                .toStream()
                // we don't need a key, so null it out
                .selectKey((k, v) -> null)
                .peek((k,v) -> log.info("Global Metrics Updated: {}", v))
                .to(OUTPUT_TOPIC, Produced.valueSerde(GLOBAL_METRICS_JSON_SERDE));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EventTicket {
        private Ticket ticket;
        private Event event;
    }

    @Data
    @AllArgsConstructor
    public static class GlobalMetrics {
        Map<String, Metrics> metrics;

        public GlobalMetrics() {
            this.metrics = new HashMap<>();
        }

        public void setArtistMetrics(String artistId, Metrics metrics) {
            this.metrics.put(artistId, metrics);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Metrics {
        public Metrics(Long ticketCount, Long streamCount) {
            if (ticketCount == null) {
                this.ticketCount = 0;
            } else {
                this.ticketCount = ticketCount.doubleValue();
            }

            if (streamCount == null) {
                this.streamCount = 0;
            } else {
                this.streamCount = streamCount.doubleValue();
            }

            this.ticketToStreamRatio = this.ticketCount / this.streamCount;
        }

        private double ticketCount;
        private double streamCount;
        private double ticketToStreamRatio;
        private String artistId;
    }
}