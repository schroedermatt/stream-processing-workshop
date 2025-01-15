package org.improving.workshop.utils;

import net.datafaker.Faker;
import org.msse.demo.mockdata.customer.address.AddressFaker;
import org.msse.demo.mockdata.customer.email.EmailFaker;
import org.msse.demo.mockdata.customer.phone.PhoneFaker;
import org.msse.demo.mockdata.customer.profile.CustomerFaker;
import org.msse.demo.mockdata.music.artist.ArtistFaker;
import org.msse.demo.mockdata.music.event.EventFaker;
import org.msse.demo.mockdata.music.stream.StreamFaker;
import org.msse.demo.mockdata.music.ticket.TicketFaker;
import org.msse.demo.mockdata.music.venue.VenueFaker;

public class DataFaker {
    private static Faker FAKER = new Faker();

    public static AddressFaker ADDRESSES = new AddressFaker(FAKER);
    public static ArtistFaker ARTISTS = new ArtistFaker(FAKER);
    public static EmailFaker EMAILS = new EmailFaker(FAKER);
    public static PhoneFaker PHONES = new PhoneFaker(FAKER);
    public static CustomerFaker CUSTOMERS = new CustomerFaker(FAKER, ADDRESSES, EMAILS, PHONES);
    public static EventFaker EVENTS = new EventFaker(FAKER);
    public static StreamFaker STREAMS = new StreamFaker(FAKER);
    public static TicketFaker TICKETS = new TicketFaker(FAKER);
    public static VenueFaker VENUES = new VenueFaker(FAKER);
}
