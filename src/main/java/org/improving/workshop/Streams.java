package org.improving.workshop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.improving.workshop.domain.customer.profile.Customer;
import org.improving.workshop.domain.music.event.Event;
import org.improving.workshop.domain.music.stream.Stream;
import org.improving.workshop.domain.music.ticket.Ticket;
import org.improving.workshop.domain.music.venue.Venue;
import org.improving.workshop.stream.PurchaseEventTicket;
import org.improving.workshop.stream.TopCustomerArtists;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.LinkedHashMap;
import java.util.Properties;

@Slf4j
public class Streams {
    public static final JsonSerde<Customer> CUSTOMER_JSON_SERDE = new JsonSerde<>(Customer.class);
    public static final JsonSerde<Venue> VENUE_JSON_SERDE = new JsonSerde<>(Venue.class);
    public static final JsonSerde<Event> EVENT_JSON_SERDE = new JsonSerde<>(Event.class);
    public static final JsonSerde<Ticket> TICKET_JSON_SERDE = new JsonSerde<>(Ticket.class);
    public static final JsonSerde<Stream> CUSTOMER_STREAM_JSON_SERDE = new JsonSerde<>(Stream.class);
    public static final JsonSerde<PurchaseEventTicket.EventStatus> REMAINING_TICKETS_JSON_SERDE = new JsonSerde<>(PurchaseEventTicket.EventStatus.class);
    public static final JsonSerde<PurchaseEventTicket.EventTicketConfirmation> TICKET_CONFIRMATION_JSON_SERDE = new JsonSerde<>(PurchaseEventTicket.EventTicketConfirmation.class);
    public static final JsonSerde<TopCustomerArtists.SortedCounterMap> COUNTER_MAP_JSON_SERDE = new JsonSerde<>(TopCustomerArtists.SortedCounterMap.class);
    public static final JsonSerde<LinkedHashMap<String, Long>> LINKED_HASH_MAP_JSON_SERDE = new JsonSerde<>(LinkedHashMap.class);

    private static final boolean logTopologyDescription = true;

    public static Properties buildProperties() {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-workshop-example-2");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "kafka-workshop-example-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-ymrq7.us-east-2.aws.confluent.cloud:9092");

        // How to connect securely to the broker(s)
        streamsConfiguration.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        streamsConfiguration.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='**' password='**';");
        streamsConfiguration.put("sasl.mechanism", "PLAIN");

        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonSerde");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches.
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Use a temporary directory for storing state, which will be automatically removed after the test.
        // streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        return streamsConfiguration;
    }

    public static void startStreams(StreamsBuilder builder) {
        Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, Streams.buildProperties());

        if (logTopologyDescription) {
            // Use the output of this log + https://zz85.github.io/kafka-streams-viz/ to vizualize your topology
            log.info("{}", topology.describe());
        }

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
}
