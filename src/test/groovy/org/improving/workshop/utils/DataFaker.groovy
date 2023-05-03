package org.improving.workshop.utils

import net.datafaker.Faker
import org.msse.demo.mockdata.customer.address.AddressFaker
import org.msse.demo.mockdata.customer.email.EmailFaker
import org.msse.demo.mockdata.customer.phone.PhoneFaker
import org.msse.demo.mockdata.customer.profile.CustomerFaker
import org.msse.demo.mockdata.music.artist.ArtistFaker
import org.msse.demo.mockdata.music.event.EventFaker
import org.msse.demo.mockdata.music.stream.StreamFaker
import org.msse.demo.mockdata.music.ticket.TicketFaker
import org.msse.demo.mockdata.music.venue.VenueFaker

class DataFaker {
    private static Faker FAKER = new Faker()

    public static def ADDRESSES = new AddressFaker(FAKER)
    public static def ARTISTS = new ArtistFaker(FAKER)
    public static def CUSTOMERS = new CustomerFaker(FAKER, ADDRESSES, EMAILS, PHONES)
    public static def EMAILS = new EmailFaker(FAKER)
    public static def EVENTS = new EventFaker(FAKER)
    public static def PHONES = new PhoneFaker(FAKER)
    public static def STREAMS = new StreamFaker(FAKER)
    public static def TICKETS = new TicketFaker(FAKER)
    public static def VENUES = new VenueFaker(FAKER)
}
