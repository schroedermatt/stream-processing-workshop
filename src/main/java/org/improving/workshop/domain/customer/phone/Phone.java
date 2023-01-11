package org.improving.workshop.domain.customer.phone;

import java.io.Serializable;

public record Phone(
        String id,
        String customerid,
        String phonetypecd,
        String primaryind,
        String timezone,
        String extnbr,
        String number) implements Serializable {}
