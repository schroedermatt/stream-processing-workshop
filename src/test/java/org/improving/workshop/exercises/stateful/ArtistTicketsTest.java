package org.improving.workshop.exercises.stateful;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.improving.workshop.Streams;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ArtistTicketsTest {
  private TopologyTestDriver driver;

  private TestInputTopic<String, Event> eventInputTopic;
  private TestInputTopic<String, Ticket> ticketInputTopic;
  private TestOutputTopic<String, Long> outputTopic;

  @BeforeEach
  public void setup() {

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    ArtistTicketCount.configureTopology(streamsBuilder);

    driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

    eventInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_EVENTS,
            Serdes.String().serializer(),
            Streams.SERDE_EVENT_JSON.serializer()
    );

    ticketInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_TICKETS,
            Serdes.String().serializer(),
            Streams.SERDE_TICKET_JSON.serializer()
    );

    outputTopic = driver.createOutputTopic(
            ArtistTicketCount.OUTPUT_TOPIC,
            Serdes.String().deserializer(),
            Serdes.Long().deserializer()
    );
  }

  @AfterEach
  public void cleanup() {
    driver.close();
  }

  @Test
  @DisplayName("artist event ticket counts")
  public void testArtistEventTicketCounts() {
    // Given an event for artist-1
    String eventId = "exciting-event-123";
    eventInputTopic.pipeInput(eventId, new Event(eventId, "artist-1", "venue-1", 10, "today"));

    // And an event for artist-2
    String eventId2 = "another-event-456";
    eventInputTopic.pipeInput(eventId2, new Event(eventId2, "artist-2", "venue-1", 10, "today"));

    // And a purchased ticket for the event
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-1", eventId));

    // When reading the output records
    var outputRecords = outputTopic.readRecordsToList();

    // Then the expected number of records were received
    assertEquals(1, outputRecords.size());

    // And artist-1 has sold 1 ticket
    var countRecord = outputRecords.get(outputRecords.size() - 1);
    assertEquals("artist-1", countRecord.key());
    assertEquals(1L, countRecord.value());

    // When purchasing 4 more tickets for artist-2's event
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-2", eventId2));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-3", eventId2));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-4", eventId2));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-5", eventId2));

    // Then the expected number of records were received
    var latestRecords = outputTopic.readRecordsToList();
    assertEquals(4, latestRecords.size());

    // And artist-2 has sold 4 tickets
    var latestCount = latestRecords.get(latestRecords.size() - 1);
    assertEquals("artist-2", latestCount.key());
    assertEquals(4L, latestCount.value());
  }
}