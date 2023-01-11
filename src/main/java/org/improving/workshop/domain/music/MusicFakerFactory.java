package org.improving.workshop.domain.music;

import org.improving.workshop.domain.music.artist.ArtistFaker;
import org.improving.workshop.domain.music.event.EventFaker;
import org.improving.workshop.domain.music.stream.StreamFaker;
import org.improving.workshop.domain.music.ticket.TicketFaker;
import org.improving.workshop.domain.music.venue.VenueFaker;
import org.springframework.stereotype.Service;

@Service
// aggregate the com.msse.demo.music fakers so you only need to inject a single faker
public record MusicFakerFactory(ArtistFaker artistFaker,
                                EventFaker eventFaker,
                                StreamFaker streamFaker,
                                TicketFaker ticketFaker,
                                VenueFaker venueFaker) {}
