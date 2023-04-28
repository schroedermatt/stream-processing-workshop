package org.improving.workshop.project;

/**
 * Class for solution to Question 1
 */
public class ArtistTicketRatio {
    public static final String OUTPUT_TOPIC = "kafka-artist-ticket-ratio";

    /**
      Inital Start
      Kyle
    */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
    }
}
