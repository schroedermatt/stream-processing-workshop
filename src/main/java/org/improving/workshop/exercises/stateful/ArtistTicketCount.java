package org.improving.workshop.exercises.stateful;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.music.event.Event;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

/**
 * Goals -
 * 1. Count the total tickets sold by each artist
 */
@Slf4j
public class ArtistTicketCount {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-artist-ticket-count";

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
        // hint: store Events in a table so that the ticket can reference them to find the Artist
        // see samples/PurchaseEventTicket for an example of creating a KTable

//        builder
//            .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
//            .peek((ticketId, ticketRequest) -> log.info("Ticket Requested: {}", ticketRequest))
//
//            // solution goes here
//
//            .peek((artistId, count) -> log.info("Artist '{}' has sold {} total tickets", artistId, count))
//            // NOTE: when using ccloud, the topic must exist or 'auto.create.topics.enable' set to true (dedicated cluster required)
//            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }
}