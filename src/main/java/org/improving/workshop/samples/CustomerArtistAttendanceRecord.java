package org.improving.workshop.samples;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.improving.workshop.Streams;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.msse.demo.mockdata.music.venue.Venue;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.*;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class CustomerArtistAttendanceRecord {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-superfans";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final JsonSerde<CustomerEvent> CUSTOMER_EVENT_JSON_SERDE = new JsonSerde<>(CustomerEvent.class);
    public static final JsonSerde<GlobalCustomerArtistAttendance> GLOBAL_CUSTOMER_ARTIST_ATTENDANCE_JSON_SERDE = new JsonSerde<>(GlobalCustomerArtistAttendance.class);

    /**
     * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
     */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {
        // store addresses in a table so that the venue/event/ticket can reference them to find capacity
        KTable<String, Address> addressesTable = builder
                .table(
                        TOPIC_DATA_DEMO_ADDRESSES,
                        Materialized
                                .<String, Address>as(persistentKeyValueStore("addresses"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ADDRESS_JSON)
                );

        // store venues in a table so that the event/ticket can reference them to find capacity
        KTable<String, Venue> venuesTable = builder
                .table(
                        TOPIC_DATA_DEMO_VENUES,
                        Materialized
                                .<String, Venue>as(persistentKeyValueStore("venues"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_VENUE_JSON)
                );

        // store events in a table so that the ticket can reference them to find capacity
        KTable<String, Event> eventsTable = builder
                .table(
                        TOPIC_DATA_DEMO_EVENTS,
                        Materialized
                            .<String, Event>as(persistentKeyValueStore("events"))
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Streams.SERDE_EVENT_JSON)
                );

        builder
            .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
            .peek((ticketId, ticketRequest) -> log.info("Ticket Requested: {}", ticketRequest))

            // rekey by eventid so we can join against the events ktable
            .selectKey((ticketId, ticketRequest) -> ticketRequest.eventid(), Named.as("rekey-t-by-eventid"))

            // join the incoming ticket to the event that it is for
            .join(
                    eventsTable,
                    (ticket, event) -> new CustomerEvent(ticket, event, null, null),
                    Joined.with(Serdes.String(), SERDE_TICKET_JSON, SERDE_EVENT_JSON)
            )

            // rekey by venue id so we can join against the venues ktable
            .selectKey((eventId, customerEvent) -> customerEvent.getEvent().venueid(), Named.as("rekey-ce-by-venueid"))

            // join the incoming ticket to the event that it is for
            .join(
                    venuesTable,
                    (customerEvent, venue) -> {
                        // update the customer event with the venue
                        customerEvent.setVenue(venue);
                        return customerEvent;
                    },
                    Joined.with(Serdes.String(), CUSTOMER_EVENT_JSON_SERDE, SERDE_VENUE_JSON)
            )

            // rekey by address id so we can join against the addresses ktable
            .selectKey((venueId, customerEvent) -> customerEvent.getVenue().addressid(), Named.as("rekey-ce-by-addressid"))

            // join the incoming ticket to the event that it is for
            .join(
                    addressesTable,
                    (customerEvent, address) -> {
                        // update the customer event with the address
                        customerEvent.setAddress(address);
                        return customerEvent;
                    },
                    Joined.with(Serdes.String(), CUSTOMER_EVENT_JSON_SERDE, SERDE_ADDRESS_JSON)
            )

            // rekey by customerid so we can keep track of the events a customer has bought tickets for
            .groupBy((addressId, customerEvent) -> customerEvent.ticket.customerid(), Grouped.valueSerde(CUSTOMER_EVENT_JSON_SERDE))

            // track
            .aggregate(
                    // initializer (doesn't have key, value supplied so the actual initialization is in the aggregator)
                    GlobalCustomerArtistAttendance::new,

                    // aggregator
                    (customerId, customerEvent, customerArtistAttendance) -> {
                        // record the customer's attendance of the event
                        customerArtistAttendance.recordEvent(customerEvent);

                        return customerArtistAttendance;
                    },

                    // ktable (materialized) configuration
                    Materialized
                            .<String, GlobalCustomerArtistAttendance>as(persistentKeyValueStore("global-customer-attendance-table"))
                            .withKeySerde(Serdes.String())
                            .withValueSerde(GLOBAL_CUSTOMER_ARTIST_ATTENDANCE_JSON_SERDE)
            )

            .toStream()
            .peek(CustomerArtistAttendanceRecord::logPretty)
            .mapValues(GlobalCustomerArtistAttendance::getSuperfanArtists)
            .filter((customerId, superfanArtists) -> superfanArtists.size() > 0)
            .mapValues((k, v) -> "'" + k +"' IS A SUPERFAN OF ARTISTS " + v)
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    @SneakyThrows
    private static void logPretty(String k, GlobalCustomerArtistAttendance v) {
        log.info("Customer '{}' Attendance Record: {}", k, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(v));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomerEvent {
        private Ticket ticket;
        private Event event;
        private Venue venue;
        private Address address;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GlobalCustomerArtistAttendance {
        // key: artist id, value: CustomerArtistAttendance
        Map<String, ArtistEventAttendance> artistEvents;

        public void recordEvent(CustomerEvent event) {
            if (this.artistEvents == null) {
                this.artistEvents = new HashMap<>();
            }

            var artistId = event.getEvent().artistid();

            // update the artist's attendance record if exists
            if (this.artistEvents.containsKey(artistId)) {
                this.artistEvents.get(artistId).recordEvent(event);

            // else initialize the artist's attendance record
            } else {
                ArtistEventAttendance artistAttendance = new ArtistEventAttendance();
                artistAttendance.recordEvent(event);
                this.artistEvents.put(artistId, artistAttendance);
            }
        }

        // artists from the artistEvents map where unique states > 1
        public List<String> getSuperfanArtists() {
            List<String> superfanArtists = new ArrayList<>();
            if (artistEvents != null) {
                for (Map.Entry<String, ArtistEventAttendance> entry : artistEvents.entrySet()) {
                    ArtistEventAttendance artistAttendance = entry.getValue();
                    if (artistAttendance != null && artistAttendance.getUniqueStates().size() > 1) {
                        // If the artist's attendance record has more than one unique state, add artistId to the list
                        superfanArtists.add(entry.getKey());
                    }
                }
            }
            return superfanArtists;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ArtistEventAttendance {
        Map<String, CustomerEvent> eventMap;
        Set<String> uniqueStates;

        public void recordEvent(CustomerEvent event) {
            if (this.eventMap == null) {
                this.eventMap = new HashMap<>();
                this.uniqueStates = new HashSet<>();
            }

            this.eventMap.put(event.getEvent().id(), event);
            this.uniqueStates.add(event.address.state());
        }
    }
}