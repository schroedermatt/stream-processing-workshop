package org.improving.workshop.exercises.stateless;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import static org.improving.workshop.Streams.*;

/**
 * Goals -
 * 1. Rekey the addresses by state
 * 2. Condense the address object into a single string, removing identifiers (address & customer)
 *      - Desired String Format: "{line1}, {line2}, {citynm}, {state} {zip5}-{zip4} {countrycd}"
 * 3. **BONUS** - Split the stream!
 *      - IF the state is 'MN', send the record to MN_OUTPUT_TOPIC ("kafka-workshop-priority-addresses")
 *      - ELSE send the record to DEFAULT_OUTPUT_TOPIC ("kafka-workshop-addresses-by-state")
 */
@Slf4j
public class AddressSortAndStringify {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String DEFAULT_OUTPUT_TOPIC = "kafka-workshop-addresses-by-state";
    public static final String MN_OUTPUT_TOPIC = "kafka-workshop-priority-addresses";

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
//            .stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON))
//            .peek((addressId, address) -> log.info("Address Received: {}, {}", addressId, address))
//
//            // DESIRED FORMAT
//            // "{line1}, {line2}, {citynm}, {state} {zip5}-{zip4} {countrycd}"
//
//            .peek((addressId, address) -> log.info("Address Processed: {}, {}", addressId, address))
//            .to(DEFAULT_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

            // BONUS - comment out the above to() and split()
    }
}