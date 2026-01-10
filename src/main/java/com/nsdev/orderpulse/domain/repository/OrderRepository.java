package com.nsdev.orderpulse.domain.repository;

import com.nsdev.orderpulse.domain.model.Order;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface OrderRepository extends MongoRepository<Order, String> {
}