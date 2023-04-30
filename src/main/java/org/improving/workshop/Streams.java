package org.improving.workshop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.improving.workshop.project.ArtistTicketRatio;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.email.Email;
import org.msse.demo.mockdata.customer.phone.Phone;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Properties;
import java.util.UUID;

@Slf4j
public class Streams {
    /////////////////////////////////////
    // data demo input topics & serdes //
    /////////////////////////////////////

    // addresses
    public static final String TOPIC_DATA_DEMO_ADDRESSES = "data-demo-addresses";
    public static final JsonSerde<Address> SERDE_ADDRESS_JSON = new JsonSerde<>(Address.class);
    // artists
    public static final String TOPIC_DATA_DEMO_ARTISTS = "data-demo-artists";
    public static final JsonSerde<Artist> SERDE_ARTIST_JSON = new JsonSerde<>(Artist.class);
    // customers
    public static final String TOPIC_DATA_DEMO_CUSTOMERS = "data-demo-customers";
    public static final JsonSerde<Customer> SERDE_CUSTOMER_JSON = new JsonSerde<>(Customer.class);
    // emails
    public static final String TOPIC_DATA_DEMO_EMAILS = "data-demo-emails";
    public static final JsonSerde<Email> SERDE_EMAIL_JSON = new JsonSerde<>(Email.class);
    // events
    public static final String TOPIC_DATA_DEMO_EVENTS = "data-demo-events";
    public static final JsonSerde<Event> SERDE_EVENT_JSON = new JsonSerde<>(Event.class);
    // phones
    public static final String TOPIC_DATA_DEMO_PHONES = "data-demo-phones";
    public static final JsonSerde<Phone> SERDE_PHONE_JSON = new JsonSerde<>(Phone.class);
    // streams
    public static final String TOPIC_DATA_DEMO_STREAMS = "data-demo-streams";
    public static final JsonSerde<Stream> SERDE_STREAM_JSON = new JsonSerde<>(Stream.class);
    // tickets
    public static final String TOPIC_DATA_DEMO_TICKETS = "data-demo-tickets";
    public static final JsonSerde<Ticket> SERDE_TICKET_JSON = new JsonSerde<>(Ticket.class);
    // venues
    public static final String TOPIC_DATA_DEMO_VENUES = "data-demo-venues";
    public static final JsonSerde<Venue> SERDE_VENUE_JSON = new JsonSerde<>(Venue.class);


    /**
     * Builds the base properties needed to start the Stream
     * @return Properties
     */
    public static Properties buildProperties() {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-workshop-example-" + UUID.randomUUID());
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "kafka-workshop-client");

        // Where to find Kafka broker(s).
        // LOCAL
         streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");

        // CONFLUENT CLOUD -- comment the below configurations out if running locally
        // TODO - WORKSHOP ATTENDEES UPDATE WITH PROVIDED BOOTSTRAP SERVER
//        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-mg1wx.us-east-2.aws.confluent.cloud:9092");
        // TODO - WORKSHOP ATTENDEES UPDATE WITH PROVIDED API KEY & SECRET
//        streamsConfiguration.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='GDESIGB2BMXPMLKJ' password='fg6FyZeRuh2M3FTuz14I0lDaHsXCeC9A1pK3aVcQR/U5QqhUnQNXSPpTt5a3vj1O';");
//        streamsConfiguration.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
//        streamsConfiguration.put("sasl.mechanism", "PLAIN");

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

    /**
     * Starts the provided StreamsBuilder instance (which contains the topology).
     * @param builder StreamsBuilder preconfigured with the Topology to execute
     */
    public static void startStreams(StreamsBuilder builder) {
        Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, Streams.buildProperties());

        // Use the output of this log + https://zz85.github.io/kafka-streams-viz/ to vizualize your topology
        log.info("{}", topology.describe());

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
