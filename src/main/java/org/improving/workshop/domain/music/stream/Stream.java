package org.improving.workshop.domain.music.stream;

import java.io.Serializable;

public record Stream(
        String id,
        String customerid,
        String artistid,
        String streamtime) implements Serializable {}
