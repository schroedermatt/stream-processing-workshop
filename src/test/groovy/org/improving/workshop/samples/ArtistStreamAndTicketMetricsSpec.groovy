package org.improving.workshop.samples


import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.msse.demo.mockdata.music.artist.Artist
import org.msse.demo.mockdata.music.event.Event
import org.msse.demo.mockdata.music.stream.Stream
import org.msse.demo.mockdata.music.ticket.Ticket
import org.msse.demo.mockdata.music.venue.Venue
import spock.lang.Specification

import static org.improving.workshop.utils.DataFaker.STREAMS
import static org.improving.workshop.utils.DataFaker.TICKETS

class ArtistStreamAndTicketMetricsSpec extends Specification {
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Artist> artistInputTopic
    TestInputTopic<String, Venue> venueInputTopic
    TestInputTopic<String, Event> eventInputTopic
    TestInputTopic<String, Ticket> ticketInputTopic
    TestInputTopic<String, Stream> streamInputTopic

    // outputs
    TestOutputTopic<String, ArtistStreamAndTicketMetrics.GlobalMetrics> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the ArtistMetrics topology (by reference)
        ArtistStreamAndTicketMetrics.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

        System.out.println(streamsBuilder.build().describe())

        artistInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ARTISTS,
                Serdes.String().serializer(),
                Streams.SERDE_ARTIST_JSON.serializer()
        )

        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        )

        venueInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_VENUES,
                Serdes.String().serializer(),
                Streams.SERDE_VENUE_JSON.serializer()
        )

        ticketInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                Serdes.String().serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        )

        streamInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_STREAMS,
                Serdes.String().serializer(),
                Streams.SERDE_STREAM_JSON.serializer()
        )

        outputTopic = driver.createOutputTopic(
                ArtistStreamAndTicketMetrics.OUTPUT_TOPIC_GLOBAL_METRICS,
                Serdes.String().deserializer(),
                ArtistStreamAndTicketMetrics.GLOBAL_METRICS_JSON_SERDE.deserializer()
        )
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def "artist ticket to stream metrics"() {
        given: 'an event'
        String artistId = "artist-1"
        String artist2Id = "artist-2"
        String eventId = "exciting-event-123"
        String venueId = "venue-1"

        artistInputTopic.pipeInput(artistId, new Artist(artistId, "Tommy Jones", "Rap"))
        artistInputTopic.pipeInput(artist2Id, new Artist(artist2Id, "Jane Johnson", "Country"))
        venueInputTopic.pipeInput(venueId, new Venue(venueId, "address-1", "Big Venue", 10))
        eventInputTopic.pipeInput(eventId, new Event(eventId, artistId, venueId, 5, "today"))

        and: 'a purchased ticket for the event'
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId))

        when: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        outputRecords.size() == 1

        and: 'artist-1 is in the metrics map and has 1 ticket and 0 streams'
        with (outputRecords.last()) {
            assert !it.key()

            // artist 1 has a ratio of 0 since no streams have come in
            assert it.value().getArtistTicketToStreamRatio().get("artist-1") == 0.0
        }

        when: 'buying more tickets and streaming the artist and another artist'
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId))
        streamInputTopic.pipeInput(STREAMS.generate("customer-1", artistId))
        streamInputTopic.pipeInput(STREAMS.generate("customer-1", artistId))
        streamInputTopic.pipeInput(STREAMS.generate("customer-1", artistId))
        streamInputTopic.pipeInput(STREAMS.generate("customer-1", artist2Id))
        streamInputTopic.pipeInput(STREAMS.generate("customer-1", artist2Id))
        streamInputTopic.pipeInput(STREAMS.generate("customer-1", artist2Id))

        then: 'the expected number of records were received'
        def latestRecords = outputTopic.readRecordsToList()
        latestRecords.size() == 10

        and: 'the final state of the metrics is accurate'
        with (latestRecords.last()) {
            assert !it.key()

            // artist 1 has a ratio of 1.66 since there have been 5 tickets and 3 streams
            assert it.value().getArtistTicketToStreamRatio().get("artist-1") == 1.6666666666666667
            // artist 2 has a ratio of 0 since there have been 0 tickets and 3 streams
            assert it.value().getArtistTicketToStreamRatio().get("artist-2") == 0.0
        }
    }
}
