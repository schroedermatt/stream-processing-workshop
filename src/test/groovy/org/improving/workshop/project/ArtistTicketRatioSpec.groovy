package org.improving.workshop.project

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.improving.workshop.samples.PurchaseEventTicket
import org.msse.demo.mockdata.music.event.Event
import org.msse.demo.mockdata.music.stream.Stream
import org.msse.demo.mockdata.music.ticket.Ticket
import spock.lang.Specification
import static org.improving.workshop.project.ArtistTicketRatio.EventTicket

class ArtistTicketRatioSpec extends Specification {
  TopologyTestDriver driver

  // inputs
  TestInputTopic<String, Event> eventInputTopic
  TestInputTopic<String, Ticket> ticketInputTopic
  TestInputTopic<String, Stream> streamInputTopic

  // outputs
  TestOutputTopic<String, PurchaseEventTicket.EventTicketConfirmation> outputTopic


    def 'setup'() {
      // instantiate new builder
      StreamsBuilder streamsBuilder = new StreamsBuilder()

      // build the RemainingEventTickets topology (by reference)
      ArtistTicketRatio.configureTopology(streamsBuilder)

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
              Streams.SERDE_TICKET_JSON.serializer()
      ) as TestInputTopic<String, Stream>

      outputTopic = driver.createOutputTopic(
              PurchaseEventTicket.OUTPUT_TOPIC,
              Serdes.String().deserializer(),
              PurchaseEventTicket.TICKET_CONFIRMATION_JSON_SERDE.deserializer()
      )
    }
    def 'cleanup'() {
      // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
      // run the test and let it cleanup, then run the test again.
      driver.close()
    }
    def "ArtistTicketRatio"() {
      given: 'a highly exclusive event (5 people allowed)'
      String eventId = "1"
      EventTicket eventTicket
      eventInputTopic.pipeInput(eventId, new Event(eventId, "1", "venue-1", 5, "today"))
      ticketInputTopic.pipeInput("1", new Ticket("1", "1", eventId, 2.33))
      ticketInputTopic.pipeInput("2", new Ticket("2", "2", eventId, 2.33))
      ticketInputTopic.pipeInput("3", new Ticket("3", "3", eventId, 2.33))
      streamInputTopic.pipeInput("1", new Stream("1", "1", "1", "10"))
      streamInputTopic.pipeInput("2", new Stream("2", "2", "1", "10"))
      streamInputTopic.pipeInput("3", new Stream("3", "3", "1", "10"))

      when: 'reading the output records'
      def outputRecords = outputTopic.readRecordsToList()

      then: 'the expected number of records were received'
      outputRecords.size() == 1
  }
}