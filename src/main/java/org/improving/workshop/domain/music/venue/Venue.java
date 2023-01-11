package org.improving.workshop.domain.music.venue;

import java.io.Serializable;

public record Venue(
        String id,
        String addressid,
        String name,
        Integer maxcapacity) implements Serializable {}