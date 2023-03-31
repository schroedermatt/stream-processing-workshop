package org.improving.workshop.exercises;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import static org.improving.workshop.Streams.*;

/**
 * Goals -
 * 1. Filter customers in a target age demographic (born in the 1990s)
 *      - The customer date format is "YYYY-MM-DD"
 */
@Slf4j
public class TargetCustomerFilter {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-target-customers";

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
//            .stream(TOPIC_DATA_DEMO_CUSTOMERS, Consumed.with(Serdes.String(), SERDE_CUSTOMER_JSON))
//
//            // BIRTHDT FORMAT "YYYY-MM-DD"
//
//            .peek((customerId, customer) -> log.info("Target Customer Found, 90s music incoming - {}", customerId))
//            // NOTE: when using ccloud, the topic must exist or 'auto.create.topics.enable' set to true (dedicated cluster required)
//            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), SERDE_CUSTOMER_JSON));
    }
}