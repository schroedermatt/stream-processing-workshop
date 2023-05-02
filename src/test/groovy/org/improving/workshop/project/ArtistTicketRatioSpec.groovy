package org.improving.workshop.project

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
import spock.lang.Specification

class ArtistTicketRatioSpec extends Specification {
  TopologyTestDriver driver

  // inputs
  TestInputTopic<String, Event> eventInputTopic
  TestInputTopic<String, Ticket> ticketInputTopic
  TestInputTopic<String, Stream> streamInputTopic
  TestInputTopic<String, Artist> artistInputTopic

  // outputs
  TestOutputTopic<String, ArtistTicketRatio.ArtistTop5Ratio> outputTopic


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
              Streams.SERDE_STREAM_JSON.serializer()
      ) as TestInputTopic<String, Stream>

      artistInputTopic = driver.createInputTopic(
              Streams.TOPIC_DATA_DEMO_ARTISTS,
              Serdes.String().serializer(),
              Streams.SERDE_ARTIST_JSON.serializer()
      ) as TestInputTopic<String, Artist>

      outputTopic = driver.createOutputTopic(
              ArtistTicketRatio.OUTPUT_TOPIC,
              Serdes.String().deserializer(),
              ArtistTicketRatio.ARTIST_NAME_RATIO_JSON_SERDE.deserializer()
      )
    }
    def 'cleanup'() {
      // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
      // run the test and let it cleanup, then run the test again.
      driver.close()
    }
    def "ArtistTicketRatio"() {
      given:
      String eventId = "1"
      String eventId2 = "2"
      String eventId3 = "3"
      artistInputTopic.pipeInput("1", new Artist("1", "Kyle", "Rock"))
      artistInputTopic.pipeInput("2", new Artist("2", "Yuha", "Roll"))
      eventInputTopic.pipeInput(eventId, new Event(eventId, "1", "venue-1", 5, "today"))
      eventInputTopic.pipeInput(eventId2, new Event(eventId2, "2", "venue-2", 5, "tomorrow"))
      ticketInputTopic.pipeInput("1", new Ticket("1", "1", eventId, 2.33))
      ticketInputTopic.pipeInput("2", new Ticket("2", "1", eventId, 2.33))
      ticketInputTopic.pipeInput("3", new Ticket("3", "2", eventId2, 2.33))
      ticketInputTopic.pipeInput("4", new Ticket("4", "2", eventId2, 2.33))
      streamInputTopic.pipeInput("1", new Stream("1", "1", "1", "10"))
      streamInputTopic.pipeInput("2", new Stream("2", "2", "2", "10"))

      when: 'reading the output records'
      def outputRecords = outputTopic.readRecordsToList()

      then: 'the expected number of records were received'
      outputRecords.size() == 2
      outputRecords.get(0).getValue().getMap().get("1").getArtistId() == "1"
      outputRecords.get(0).getValue().getMap().get("1").getArtistRatio() == 2
      outputRecords.get(0).getValue().getMap().get("1").getName() == "Kyle"

      outputRecords.get(0).getValue().getMap().get("1").getArtistId() == "1"
      outputRecords.get(0).getValue().getMap().get("1").getArtistRatio() == 2
      outputRecords.get(0).getValue().getMap().get("1").getName() == "Kyle"

      outputRecords.get(1).getValue().getMap().get("2").getArtistId() == "2"
      outputRecords.get(1).getValue().getMap().get("2").getArtistRatio() == 2
      outputRecords.get(1).getValue().getMap().get("2").getName() == "Yuha"

      when:
      streamInputTopic.pipeInput("3", new Stream("3", "1", "1", "10"))
      streamInputTopic.pipeInput("4", new Stream("4", "3", "2", "10"))
      outputRecords = outputTopic.readRecordsToList()


      then:
      outputRecords.size() == 2
      outputRecords.get(0).getValue().getMap().get("1").getArtistId() == "1"
      outputRecords.get(0).getValue().getMap().get("1").getArtistRatio() == 1
      outputRecords.get(0).getValue().getMap().get("1").getName() == "Kyle"

      outputRecords.get(0).getValue().getMap().get("2").getArtistId() == "2"
      outputRecords.get(0).getValue().getMap().get("2").getArtistRatio() == 2
      outputRecords.get(0).getValue().getMap().get("2").getName() == "Yuha"

      outputRecords.get(1).getValue().getMap().get("1").getArtistId() == "1"
      outputRecords.get(1).getValue().getMap().get("1").getArtistRatio() == 1
      outputRecords.get(1).getValue().getMap().get("1").getName() == "Kyle"

      outputRecords.get(1).getValue().getMap().get("2").getArtistId() == "2"
      outputRecords.get(1).getValue().getMap().get("2").getArtistRatio() == 1
      outputRecords.get(1).getValue().getMap().get("2").getName() == "Yuha"

      when:
      artistInputTopic.pipeInput("3", new Artist("3", "Jerry", "Tumble"))
      eventInputTopic.pipeInput(eventId3, new Event(eventId3, "3", "venue-1", 5, "today"))
      ticketInputTopic.pipeInput("5", new Ticket("5", "1", eventId3, 2.33))
      ticketInputTopic.pipeInput("6", new Ticket("6", "1", eventId3, 2.33))
      streamInputTopic.pipeInput("5", new Stream("5", "2", "3", "10"))
      outputRecords = outputTopic.readRecordsToList()


      then:
      outputRecords.size() == 1
      outputRecords.get(0).getValue().getMap().get("1").getArtistId() == "1"
      outputRecords.get(0).getValue().getMap().get("1").getArtistRatio() == 1
      outputRecords.get(0).getValue().getMap().get("1").getName() == "Kyle"

      outputRecords.get(0).getValue().getMap().get("2").getArtistId() == "2"
      outputRecords.get(0).getValue().getMap().get("2").getArtistRatio() == 1
      outputRecords.get(0).getValue().getMap().get("2").getName() == "Yuha"

      outputRecords.get(0).getValue().getMap().get("3").getArtistId() == "3"
      outputRecords.get(0).getValue().getMap().get("3").getArtistRatio() == 2
      outputRecords.get(0).getValue().getMap().get("3").getName() == "Jerry"
    }
}