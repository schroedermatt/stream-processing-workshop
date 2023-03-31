package org.improving.workshop.exercises.stateless;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import static org.improving.workshop.Streams.*;

/**
 * Goals -
 * 1. Rekey the address stream by state
 * 2. Condense the address into a single string, removing identifiers (address & customer)
 *      - String Format: "{line1}, {line2}, {citynm}, {state} {zip5}-{zip4} {countrycd}"
 */
@Slf4j
public class AddressSortAndStringify {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-addresses-by-state";

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
            .stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON))
            .peek((addressId, address) -> log.info("Address Received: {}, {}", addressId, address))

            // DESIRED FORMAT
            // "{line1}, {line2}, {citynm}, {state} {zip5}-{zip4} {countrycd}"
            .map((id, address) -> {
                String add = String.format("%s, %s, %s, %s %s-%s %s",
                        address.line1(), address.line2(), address.citynm(), address.state(), address.zip5(), address.zip4(), address.countrycd());
                return new KeyValue<>(address.state(), add);
            })

            .peek((addressId, address) -> log.info("Address Processed: {}, {}", addressId, address))
            // NOTE: when using ccloud, the topic must exist or 'auto.create.topics.enable' set to true (dedicated cluster required)
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }
}