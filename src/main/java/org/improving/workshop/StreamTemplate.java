package org.improving.workshop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import static org.improving.workshop.Streams.startStreams;

@Slf4j
public class StreamTemplate {
    // Reference TOPIC_DATA_DEMO_* properties in Streams
    public static final String INPUT_TOPIC = "data-demo-{entity}";
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-{unique-topic-name}";

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
        builder
            .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .peek((key, value) -> log.info("Event Received: {},{}", key, value))

            // add topology here

            // NOTE: when using ccloud, the topic must exist or 'auto.create.topics.enable' set to true (dedicated cluster required)
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

}