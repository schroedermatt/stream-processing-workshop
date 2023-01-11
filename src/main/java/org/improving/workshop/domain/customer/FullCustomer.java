package org.improving.workshop.domain.customer;

import org.improving.workshop.domain.customer.address.Address;
import org.improving.workshop.domain.customer.email.Email;
import org.improving.workshop.domain.customer.phone.Phone;
import org.improving.workshop.domain.customer.profile.Customer;

public record FullCustomer(
        Customer customer,
        Address address,
        Email email,
        Phone phone
) {
}
