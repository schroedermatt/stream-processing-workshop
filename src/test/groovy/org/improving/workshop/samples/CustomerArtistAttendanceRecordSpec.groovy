package org.improving.workshop.samples

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.msse.demo.mockdata.customer.address.Address
import org.msse.demo.mockdata.music.event.Event
import org.msse.demo.mockdata.music.ticket.Ticket
import org.msse.demo.mockdata.music.venue.Venue
import org.springframework.kafka.support.serializer.JsonSerde
import spock.lang.Specification

import static org.improving.workshop.utils.DataFaker.ADDRESSES
import static org.improving.workshop.utils.DataFaker.TICKETS

class CustomerArtistAttendanceRecordSpec extends Specification {
    TopologyTestDriver driver

    // inputs - tables
    TestInputTopic<String, Event> eventInputTopic
    TestInputTopic<String, Address> addressInputTopic
    TestInputTopic<String, Venue> venueInputTopic

    // inputs - stream
    TestInputTopic<String, Ticket> ticketInputTopic

    // outputs
    TestOutputTopic<String, String> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the ArtistMetrics topology (by reference)
        CustomerArtistAttendanceRecord.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

        eventInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        )

        addressInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ADDRESSES,
                Serdes.String().serializer(),
                Streams.SERDE_ADDRESS_JSON.serializer()
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

        outputTopic = driver.createOutputTopic(
                CustomerArtistAttendanceRecord.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        )
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def "customer artist attendance"() {
        given: 'two addresses'
        def add = generateAddress("a1", "MN")
        addressInputTopic.pipeInput(add.id(), add)
        def add2 = generateAddress("a2", "WI")
        addressInputTopic.pipeInput(add2.id(), add2)

        and: 'two venues tied to those addresses'
        def ven = new Venue("ven-1", add.id(), "test venue", 10)
        venueInputTopic.pipeInput(ven.id(), ven)
        def ven2 = new Venue("ven-2", add2.id(), "test venue 2", 10)
        venueInputTopic.pipeInput(ven2.id(), ven2)

        and: 'two events booked at those venues'
        def event = new Event("e1", "artist-1", ven.id(), 5, "today")
        eventInputTopic.pipeInput(event.id(), event)
        def event2 = new Event("e2", "artist-1", ven2.id(), 5, "today")
        eventInputTopic.pipeInput(event2.id(), event2)

        and: 'a purchased ticket for the first event'
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", event.id()))

        expect: 'reading the output records - there are none. The customer is not a superfan.'
        outputTopic.readRecordsToList().isEmpty()

        when: 'purchasing a ticket the second event (in another state)'
        ticketInputTopic.pipeInput(TICKETS.generate("customer-1", event2.id()))

        then: 'SUPERFAN STATUS ACHIEVED!'
        def superfanRecord = outputTopic.readRecordsToList().first()
        superfanRecord.key() == 'customer-1'
        superfanRecord.value() == "'customer-1' IS A SUPERFAN OF ARTISTS [artist-1]"
    }

    Address generateAddress(String id, String state) {
        return new Address(id, null, null, null, null, null, null, state, null, null, null)
    }
}
