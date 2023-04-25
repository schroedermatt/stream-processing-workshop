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
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class ArtistEventsGrossEarnings {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-artist-events-gross-earnings";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final JsonSerde<EventTicket> EVENT_TICKET_JSON_SERDE = new JsonSerde<>(EventTicket.class);
    public static final JsonSerde<ArtistEventEarnings> ARTIST_EVENT_EARNINGS_JSON_SERDE = new JsonSerde<>(ArtistEventEarnings.class);

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
        builder
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
            .groupBy((s, eventTicket) -> eventTicket.getEvent().artistid(), Grouped.with(Serdes.String(), EVENT_TICKET_JSON_SERDE))
            .aggregate(
                    // initializer (doesn't have key, value supplied so the actual initialization is in the aggregator)
                    ArtistEventEarnings::new,

                    // aggregator
                    (artistId, eventTicket, artistEventEarnings) -> {
                        // update the artist's event earnings
                        artistEventEarnings.recordEarnings(eventTicket.event.id(), eventTicket.ticket.price());

                        return artistEventEarnings;
                    },

                    // ktable (materialized) configuration
                    Materialized
                            .<String, ArtistEventEarnings>as(persistentKeyValueStore("artist-earnings-table"))
                            .withKeySerde(Serdes.String())
                            .withValueSerde(ARTIST_EVENT_EARNINGS_JSON_SERDE)
            )
            .toStream()
            .peek(ArtistEventsGrossEarnings::logEarningsPretty)
            .to(OUTPUT_TOPIC, Produced.valueSerde(ARTIST_EVENT_EARNINGS_JSON_SERDE));
    }

    @SneakyThrows
    private static void logEarningsPretty(Object k, ArtistEventEarnings v) {
        log.info("Arist '{}' Earnings Updated: {}", k, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(v));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EventTicket {
        private Ticket ticket;
        private Event event;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ArtistEventEarnings {
        Map<String, Double> eventEarnings;

        public void recordEarnings(String eventId, Double ticketPrice) {
            if (eventEarnings == null) {
                this.eventEarnings = new HashMap<>();
            }

            if (eventEarnings.containsKey(eventId)) {
                // add ticket price to existing event earnings
                this.eventEarnings.put(eventId, this.eventEarnings.get(eventId) + ticketPrice);
            } else {
                // first ticket sold! initialize event earnings
                this.eventEarnings.put(eventId, ticketPrice);
            }
        }
    }
}