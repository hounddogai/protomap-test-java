package com.acme.finance.domains.banking;

import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class BankAccountWorkflowApplicationEvents {
    private final CustomerServiceGrpc.CustomerServiceStub customersCustomerService;

    public BankAccountWorkflowApplicationEvents(CustomerServiceGrpc.CustomerServiceStub customersCustomerService) {
        this.customersCustomerService = customersCustomerService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(BankAccountWorkflowApplication.ResumeBankAccountWorkflowStage1Requested event) {
        event.context().run(() -> {
            try {
                customersCustomerService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .updateCustomer(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
