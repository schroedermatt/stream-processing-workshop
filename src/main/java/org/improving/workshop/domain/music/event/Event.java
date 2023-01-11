package org.improving.workshop.domain.music.event;

import java.io.Serializable;

public record Event(
        String id,
        String artistid,
        String venueid,
        Integer capacity,
        String eventdate) implements Serializable {}
