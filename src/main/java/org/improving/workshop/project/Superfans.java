package org.improving.workshop.project;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.improving.workshop.samples.PurchaseEventTicket;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.*;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;
import static org.msse.demo.mockdata.music.ticket.Ticket.*;

/**
 * Class for solution to Question 2
 *
 * Step 1 Load Address data
 * Step 2 Load Venue data
 * Step 3 Load Event data
 * Step 4 Load and ReKey Ticket data by EventID
 * Step 5 Join Events and Tickets(EventID) and send to Event-Ticket(EventID) Stream
 * Step 6 ReKey the Event-Ticket(EventID) Stream by VenueID
 * Step 7 Join the Event-Ticket(VenueID) Stream with Venue and send to Event-Ticket-Venue(VenueID) Stream
 * Step 8 ReKey Event-Ticket-Venue(VenueID) Stream by AddressID
 * Step 9 Join Event-Ticket-Venue(VenueID) Stream with Address and send to Event-Ticket-Venue-Address(AddressID) Stream
 * Step 10 Append State to CustomerID
 * Step 11 Global DB with CustomerID-ArtistID pairs and their list of states
 * Step 12 Filter by pairings with more than one state
*/

@Slf4j
public class Superfans {

    public static final String SUPERFAN_TOPIC = "superfan-topic";
    public static final JsonSerde<EventTicket> EVENT_TICKET_JSON_SERDE = new JsonSerde<>(EventTicket.class);
    public static final JsonSerde<EventTicketVenue> EVENT_TICKET_VENUE_JSON_SERDE = new JsonSerde<>(EventTicketVenue.class);
    public static final JsonSerde<EventTicketVenueAddress> EVENT_TICKET_VENUE_ADDRESS_JSON_SERDE = new JsonSerde<>(EventTicketVenueAddress.class);
    public static final JsonSerde<FanWithArtistStateMap> FAN_WITH_ARTIST_STATE_MAP_JSON_SERDE = new JsonSerde<>(FanWithArtistStateMap.class);
    public static final JsonSerde<Superfan> SUPERFAN_JSON_SERDE = new JsonSerde<>(Superfan.class);


    static void configureTopology(final StreamsBuilder builder) {
        KTable<String, Event> eventsTable = builder
                .table(
                        TOPIC_DATA_DEMO_EVENTS,
                        Materialized
                                .<String, Event>as(persistentKeyValueStore("events"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_EVENT_JSON)
                );

        KTable<String, Venue> venuesTable = builder
                .table(
                        TOPIC_DATA_DEMO_VENUES,
                        Materialized
                                .<String, Venue>as(persistentKeyValueStore("venues"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_VENUE_JSON)
                );

        KTable<String, Address> addressTable = builder
                .table(
                        TOPIC_DATA_DEMO_ADDRESSES,
                        Materialized
                                .<String, Address>as(persistentKeyValueStore("address"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ADDRESS_JSON)
                );
//Join Ticket with Event
        KStream<String, EventTicketVenueAddress> eventTicketVenueAddressStream = builder.stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .selectKey((ticketId, ticket) -> ticket.eventid())
                .join(eventsTable, (ticket, event) -> new EventTicket(event, ticket), Joined.with(Serdes.String(), SERDE_TICKET_JSON, SERDE_EVENT_JSON))
                .selectKey((eventID, eventticket) -> eventticket.event.venueid())
                .join(venuesTable, (eventticket, venue) -> new EventTicketVenue(eventticket.event, eventticket.ticket, venue), Joined.with(Serdes.String(), EVENT_TICKET_JSON_SERDE, SERDE_VENUE_JSON))
                .selectKey((venueID, eventticketvenue) -> eventticketvenue.venue.addressid())
                .join(addressTable, (eventTicketVenue, address) -> new EventTicketVenueAddress(eventTicketVenue.event, eventTicketVenue.ticket, eventTicketVenue.venue, address), Joined.with(Serdes.String(), EVENT_TICKET_VENUE_JSON_SERDE, SERDE_ADDRESS_JSON))
                ;

        eventTicketVenueAddressStream.groupBy((k, eventTicketVenueAddress) -> eventTicketVenueAddress.ticket.customerid(), Grouped.with(Serdes.String(), EVENT_TICKET_VENUE_ADDRESS_JSON_SERDE))
                .aggregate(() -> {
                    FanWithArtistStateMap aggregate = new FanWithArtistStateMap();
                    aggregate.artistToStateMap = new HashMap<>();
                    return aggregate;
                }, (customerId, eventTicketVenueAddress, aggregate) -> {
                    if (aggregate.artistToStateMap.containsKey(eventTicketVenueAddress.event.artistid())) {
                        if (!aggregate.artistToStateMap.get(eventTicketVenueAddress.event.artistid()).contains(eventTicketVenueAddress.address.state())) {
                            aggregate.artistToStateMap.get(eventTicketVenueAddress.event.artistid()).add(eventTicketVenueAddress.address.state());
                        }
                    } else {
                        List<String> stateList = new ArrayList<>();
                        stateList.add(eventTicketVenueAddress.address.state());
                        aggregate.artistToStateMap.put(eventTicketVenueAddress.event.artistid(), stateList);
                    }
                    aggregate.artistId = eventTicketVenueAddress.event.artistid();
                    return aggregate;
                }, Materialized.with(Serdes.String(), FAN_WITH_ARTIST_STATE_MAP_JSON_SERDE))
                .toStream()
                .filter((customerId, fanWithStateMap) -> fanWithStateMap.artistToStateMap.get(fanWithStateMap.artistId).size() >= 2)
                .mapValues((customerId, fanWithStateMap) -> new Superfan(customerId, fanWithStateMap.artistId, fanWithStateMap.artistToStateMap.get(fanWithStateMap.artistId)))
                .to(SUPERFAN_TOPIC, Produced.with(Serdes.String(), SUPERFAN_JSON_SERDE))
        ;



    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class FanWithArtistStateMap {
        String artistId;
        Map<String, List<String>> artistToStateMap;
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Superfan {
        String customerID;
        String artistID;
        List<String> stateList;
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EventTicket {
        Event event;
        Ticket ticket;
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EventTicketVenue {
        Event event;
        Ticket ticket;
        Venue venue;
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EventTicketVenueAddress {
        Event event;
        Ticket ticket;
        Venue venue;
        Address address;
    }

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }
}
