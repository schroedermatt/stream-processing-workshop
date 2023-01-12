package org.improving.workshop.domain.music.event;

import net.datafaker.Faker;
import org.improving.workshop.domain.faker.BaseFaker;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class EventFaker extends BaseFaker {
  public EventFaker(Faker faker) {
    super(faker);
  }

  public Event generate(String artistId, String venueId, int maxCapacity) {
    return generate(randomId(), artistId, venueId, maxCapacity);
  }

  public Event generate(String eventId, String artistId, String venueId, int maxCapacity) {
    return new Event(
            eventId,
            artistId,
            venueId,
            maxCapacity,
            faker.date().future(250, TimeUnit.DAYS).toString()
    );
  }
}
