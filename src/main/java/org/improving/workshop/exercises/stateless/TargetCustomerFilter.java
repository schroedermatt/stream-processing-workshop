package org.improving.workshop.exercises.stateless;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import static org.improving.workshop.Streams.*;

/**
 * Goals -
 * 1. Filter customers in a target age demographic (born in the 1990s)
 *      - The customer date format is "YYYY-MM-DD"
 * 2. **BONUS** - Merge streams!
 *      - There is an existing LEGACY_INPUT_TOPIC ("data-demo-legacy-customers"), merge the legacy customers
 *      into TOPIC_DATA_DEMO_CUSTOMERS ("data-demo-customers") to ensure you're analyzing ALL customers.
 */
@Slf4j
public class TargetCustomerFilter {
    public static final String LEGACY_INPUT_TOPIC = "data-demo-legacy-customers";

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
        var legacyStream = builder
            .stream(LEGACY_INPUT_TOPIC, Consumed.with(Serdes.String(), SERDE_CUSTOMER_JSON));

        builder
            .stream(TOPIC_DATA_DEMO_CUSTOMERS, Consumed.with(Serdes.String(), SERDE_CUSTOMER_JSON))

            .merge(legacyStream, Named.as("legacy-merge"))

            // BIRTHDT FORMAT "YYYY-MM-DD"
            .filter((s, customer) -> customer.birthdt().startsWith("199"))

            .peek((customerId, customer) -> log.info("Target Customer Found, 90s music incoming - {}", customerId))
            // NOTE: when using ccloud, the topic must exist or 'auto.create.topics.enable' set to true (dedicated cluster required)
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), SERDE_CUSTOMER_JSON));
    }
}