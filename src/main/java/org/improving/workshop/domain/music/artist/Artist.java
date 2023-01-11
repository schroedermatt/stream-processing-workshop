package org.improving.workshop.domain.music.artist;

import java.io.Serializable;

public record Artist(
        String id,
        String name,
        String genre) implements Serializable {}