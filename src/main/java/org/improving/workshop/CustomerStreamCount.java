package org.improving.workshop;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.JsonSerde;

public class CustomerStreamCount {
    static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CustomerStreamCount.class);

    static final JsonSerde<ObjectNode> JSON_NODE_SERDE = new JsonSerde<>(ObjectNode.class);

    static final String inputTopic = "data-demo-streams";
    static final String outputTopic = "kafka-workshop-customer-stream-count";

    /**
     * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
     */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // define the processing topology of the Streams application.
        buildCustomerStreamCounter(builder);

        Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, StreamsProperties.build());
        logger.info("{}", topology.describe());

        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();

        // Now run the processing topology via `start()` to begin processing its input data.
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static void buildCustomerStreamCounter(final StreamsBuilder builder) {
        final KStream<String, ObjectNode> artists = builder
                .stream(inputTopic, Consumed.with(Serdes.String(), JSON_NODE_SERDE))
                .peek((streamId, stream) -> logger.info("Stream Received: {}", stream));

        final KTable<String, Long> streamCounts = artists
                // rekey so that the groupBy is by customerid and not streamid
                .selectKey((streamId, stream) -> stream.get("customerid").textValue())
                .groupBy((customerId, stream) -> customerId)
                .count();

        // Write the `KTable<String, Long>` to the output topic.
        streamCounts
                .toStream()
                .peek((customerId, count) -> logger.info("Customer '{}' has {} streams", customerId, count));
                // NOTE: when using ccloud, the topic must exist or 'auto.create.topics.enable' set to true (dedicated cluster required)
                // .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
    }

}