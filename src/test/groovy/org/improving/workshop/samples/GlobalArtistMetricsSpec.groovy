package org.improving.workshop.samples


import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.msse.demo.mockdata.music.event.Event
import org.msse.demo.mockdata.music.stream.Stream
import org.msse.demo.mockdata.music.ticket.Ticket
import spock.lang.Specification

import static org.improving.workshop.utils.DataFaker.STREAMS
import static org.improving.workshop.utils.DataFaker.TICKETS

class GlobalArtistMetricsSpec extends Specification {
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Event> eventInputTopic
    TestInputTopic<String, Ticket> ticketInputTopic
    TestInputTopic<String, Stream> streamInputTopic

    // outputs
    TestOutputTopic<String, GlobalArtistMetrics.GlobalMetrics> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the ArtistMetrics topology (by reference)
        GlobalArtistMetrics.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
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
                GlobalArtistMetrics.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                GlobalArtistMetrics.GLOBAL_METRICS_JSON_SERDE.deserializer()
        )
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def "global artist metrics"() {
        given: 'an event'
        String artistId = "artist-1"
        String artist2Id = "artist-2"
        String eventId = "exciting-event-123"

        eventInputTopic.pipeInput(eventId, new Event(eventId, artistId, "venue-1", 5, "today"))

        and: 'a purchased ticket for the event'
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId))

        when: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        outputRecords.size() == 1

        and: 'artist-1 is in the metrics map and has 1 ticket and 0 streams'
        with (outputRecords.last()) {
            assert !it.key()

            // artist 1
            assert it.value().getMetrics().get(artistId).ticketCount == 1
            assert it.value().getMetrics().get(artistId).streamCount == 0
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

            // artist 1
            assert it.value().getMetrics().get(artistId).ticketCount == 5
            assert it.value().getMetrics().get(artistId).streamCount == 3

            // artist 2
            assert it.value().getMetrics().get(artist2Id).ticketCount == 0
            assert it.value().getMetrics().get(artist2Id).streamCount == 3
        }
    }
}
