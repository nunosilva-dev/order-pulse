package com.nsdev.orderpulse.web.controller;

import com.nsdev.orderpulse.domain.model.Order;
import com.nsdev.orderpulse.domain.service.OrderService;
import com.nsdev.orderpulse.web.dto.OrderRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

/**
 * REST Controller for managing Orders.
 * <p>
 * This entry point handles HTTP requests related to the Order lifecycle.
 * It delegates business logic to the {@link OrderService}.
 */
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    /**
     * Creates a new Order in the system using the Resilient (Outbox) pattern.
     *
     * @param request The data transfer object containing order details.
     * @return The created {@link Order} entity.
     */
    @PostMapping("/create")
    @ResponseStatus(HttpStatus.CREATED)
    public Order createOrder(@RequestBody @Valid OrderRequest request) {
        return orderService.createOrder(request);
    }
}