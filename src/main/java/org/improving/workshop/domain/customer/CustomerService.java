package org.improving.workshop.domain.customer;

public interface CustomerService {
    FullCustomer createCustomer();
    FullCustomer createCustomer(String customerId);
    long customerCount();
}
