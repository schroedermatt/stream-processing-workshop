package org.improving.workshop.exercises.stateful;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.improving.workshop.Streams;

import static org.improving.workshop.Streams.TOPIC_DATA_DEMO_STREAMS;
import static org.improving.workshop.Streams.startStreams;

/**
 * Goals -
 * 1. Count the total streams (listens) for each customer
 */
@Slf4j
public class CustomerStreamCount {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-customer-stream-count";

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
//        builder
//            // consume events from INPUT_TOPIC
//            .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), Streams.SERDE_STREAM_JSON))
//            .peek((streamId, stream) -> log.info("Stream Received: {}", stream))
//
//            // solution goes here
//
//            // NOTE: when using ccloud, the topic must exist or 'auto.create.topics.enable' set to true (dedicated cluster required)
//            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

}