package org.improving.workshop.stream

import net.datafaker.Faker
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.improving.workshop.domain.music.event.Event
import org.improving.workshop.domain.music.event.EventFaker
import org.improving.workshop.domain.music.ticket.Ticket
import org.improving.workshop.domain.music.ticket.TicketFaker
import spock.lang.Specification

class PurchaseEventTicketSpec extends Specification {
    TopologyTestDriver driver
    EventFaker eventFaker
    TicketFaker ticketFaker

    TestInputTopic<String, Event> eventInputTopic
    TestInputTopic<String, Ticket> ticketInputTopic
    TestOutputTopic<String, PurchaseEventTicket.EventTicketConfirmation> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the RemainingEventTickets topology (by reference)
        PurchaseEventTicket.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

        eventInputTopic = driver.createInputTopic(
                PurchaseEventTicket.INPUT_TOPIC_EVENTS,
                Serdes.String().serializer(),
                Streams.EVENT_JSON_SERDE.serializer()
        )

        ticketInputTopic = driver.createInputTopic(
                PurchaseEventTicket.INPUT_TOPIC_TICKETS,
                Serdes.String().serializer(),
                Streams.TICKET_JSON_SERDE.serializer()
        )

        outputTopic = driver.createOutputTopic(
                PurchaseEventTicket.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                Streams.TICKET_CONFIRMATION_JSON_SERDE.deserializer()
        )

        eventFaker = new EventFaker(new Faker())
        ticketFaker = new TicketFaker(new Faker())
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def "purchase event tickets"() {
        given: 'a highly exclusive event (5 people allowed)'
        String eventId = "exciting-event-123"
        eventInputTopic.pipeInput(eventId, eventFaker.generate("artist-1", "venue-2", 5))

        and: 'a purchased ticket for the event'
        ticketInputTopic.pipeInput(ticketFaker.generate("customer-1", eventId))

        when: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        outputRecords.size() == 1

        and: 'CONFIRMED response received and there are 4 remaining tickets'
        def confirmation = outputRecords.last()

        confirmation.key() == eventId
        confirmation.value().confirmationStatus == 'CONFIRMED'
        confirmation.value().remainingTickets == 4
        confirmation.value().event
        confirmation.value().ticketRequest
        confirmation.value().confirmationId

        when: 'purchasing 4 more tickets for the event'
        ticketInputTopic.pipeInput(ticketFaker.generate("customer-2", eventId))
        ticketInputTopic.pipeInput(ticketFaker.generate("customer-3", eventId))
        ticketInputTopic.pipeInput(ticketFaker.generate("customer-4", eventId))
        ticketInputTopic.pipeInput(ticketFaker.generate("customer-5", eventId))

        then: 'the expected number of records were received'
        def latestRecords = outputTopic.readRecordsToList()
        latestRecords.size() == 4

        and: 'all tickets were CONFIRMED'
        latestRecords.each {
            assert it.key() == eventId
            assert it.value().confirmationStatus == 'CONFIRMED'
        }

        and: 'there are zero tickets remaining'
        latestRecords.last().value().remainingTickets == 0

        when: 'purchasing additional tickets'
        ticketInputTopic.pipeInput(ticketFaker.generate("customer-112", eventId))
        ticketInputTopic.pipeInput(ticketFaker.generate("customer-113", eventId))
        ticketInputTopic.pipeInput(ticketFaker.generate("customer-114", eventId))

        then: 'the expected number of records were received'
        def finalRecords = outputTopic.readRecordsToList()
        finalRecords.size() == 3

        and: 'all tickets were REJECTED'
        finalRecords.each {
            assert it.key() == eventId
            assert it.value().confirmationStatus == 'REJECTED'
        }

        and: 'there are zero tickets remaining'
        finalRecords.last().value().remainingTickets < 0
    }
}
