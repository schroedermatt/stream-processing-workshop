package org.improving.workshop.samples;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.improving.workshop.Streams;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PurchaseEventTicketTest {

  private final static Serializer<String> stringSerializer = Serdes.String().serializer();
  private final static Deserializer<String> stringDeserializer = Serdes.String().deserializer();

  private TopologyTestDriver driver;

  // inputs
  private TestInputTopic<String, Event> eventInputTopic;
  private TestInputTopic<String, Ticket> ticketInputTopic;

  // outputs
  private TestOutputTopic<String, PurchaseEventTicket.EventTicketConfirmation> outputTopic;

  @BeforeEach
  void setup() {
    // instantiate new builder
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    // build the RemainingEventTickets topology (by reference)
    PurchaseEventTicket.configureTopology(streamsBuilder);

    // build the TopologyTestDriver
    driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

    eventInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_EVENTS,
            stringSerializer,
            Streams.SERDE_EVENT_JSON.serializer()
    );

    ticketInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_TICKETS,
            stringSerializer,
            Streams.SERDE_TICKET_JSON.serializer()
    );

    outputTopic = driver.createOutputTopic(
            PurchaseEventTicket.OUTPUT_TOPIC,
            stringDeserializer,
            PurchaseEventTicket.TICKET_CONFIRMATION_JSON_SERDE.deserializer()
    );
  }

  @AfterEach
  void cleanup() {
    // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
    // run the test and let it cleanup, then run the test again.
    driver.close();
  }

  @Test
  @DisplayName("purchase event tickets")
  void purchaseEventTickets() {
    //a highly exclusive event (5 people allowed)
    String eventId = "exciting-event-123";

    eventInputTopic.pipeInput(eventId, new Event(eventId, "artist-1", "venue-1", 5, "today"));

    //a purchased ticket for the event
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-1", eventId));

    //reading the output records'
    var outputRecords = outputTopic.readRecordsToList();

    //the expected number of records were received
    assertEquals(1, outputRecords.size());

    //CONFIRMED response received and there are 4 remaining tickets
    TestRecord<String, PurchaseEventTicket.EventTicketConfirmation> confirmation = outputRecords.getLast();

    assertEquals(eventId, confirmation.key());
    assertEquals("CONFIRMED", confirmation.value().getConfirmationStatus());
    assertEquals(4, confirmation.value().getRemainingTickets());
    assertNotNull(confirmation.value().getEvent());
    assertNotNull(confirmation.value().getTicketRequest());
    assertNotNull(confirmation.value().getConfirmationId());

    //when: 'purchasing 4 more tickets for the event'
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-2", eventId));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-3", eventId));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-4", eventId));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-5", eventId));

    //then the expected number of records were received
    var latestRecords = outputTopic.readRecordsToList();
    assertEquals(4, latestRecords.size());

    //and all tickets were CONFIRMED
    latestRecords.forEach(it -> {
      assertEquals(eventId, it.key());
      assertEquals("CONFIRMED", it.value().getConfirmationStatus());
    });

   // and there are zero tickets remaining
    assertEquals(0, latestRecords.getLast().value().getRemainingTickets());

    // when purchasing additional tickets
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-112", eventId));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-113", eventId));
    ticketInputTopic.pipeInput(DataFaker.TICKETS.generate("customer-114", eventId));

    // then the expected number of records were received
    var finalRecords = outputTopic.readRecordsToList();

    assertEquals(3, finalRecords.size());

    // and all tickets were REJECTED
    finalRecords.forEach(it -> {
      assertEquals(eventId, it.key());
      assertEquals("REJECTED", it.value().getConfirmationStatus());
    });

    // and there are zero tickets remaining
    assertTrue(finalRecords.getLast().value().getRemainingTickets() < 0);
  }

}
