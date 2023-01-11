package org.improving.workshop.domain.music.event;

public record EventRequest(
        String artistid,
        String venueid) {}
