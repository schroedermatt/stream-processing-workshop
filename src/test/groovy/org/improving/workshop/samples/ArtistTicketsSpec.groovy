package org.improving.workshop.samples

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.msse.demo.mockdata.music.event.Event
import org.msse.demo.mockdata.music.ticket.Ticket
import spock.lang.Specification

import static org.improving.workshop.utils.DataFaker.TICKETS

class ArtistTicketsSpec extends Specification {
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Event> eventInputTopic
    TestInputTopic<String, Ticket> ticketInputTopic

    // outputs - artistid, count
    TestOutputTopic<String, Long> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the RemainingEventTickets topology (by reference)
        ArtistTicketCount.configureTopology(streamsBuilder)
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

        outputTopic = driver.createOutputTopic(
                ArtistTicketCount.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                Serdes.Long().deserializer()
        )
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def "artist event ticket counts"() {
        given: 'an event for artist-1'
        String eventId = "exciting-event-123"
        eventInputTopic.pipeInput(eventId, new Event(eventId, "artist-1", "venue-1", 10, "today"))

        and: 'an event for artist-2'
        String eventId2 = "another-event-456"
        eventInputTopic.pipeInput(eventId2, new Event(eventId2, "artist-2", "venue-1", 10, "today"))

        and: 'a purchased ticket for the event'
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", eventId))

        when: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        outputRecords.size() == 1

        and: 'artist-1 has sold 1 ticket'
        def count = outputRecords.last()
        count.key() == "artist-1"
        count.value() == 1

        when: 'purchasing 4 more tickets for the event'
        ticketInputTopic.pipeInput(TICKETS.generate("customer-2", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-3", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-4", eventId2))
        ticketInputTopic.pipeInput(TICKETS.generate("customer-5", eventId2))

        then: 'the expected number of records were received'
        def latestRecords = outputTopic.readRecordsToList()
        latestRecords.size() == 4

        and: 'artist-1 has sold 1 ticket'
        latestRecords.last().key() == "artist-2"
        latestRecords.last().value() == 4
    }
}
