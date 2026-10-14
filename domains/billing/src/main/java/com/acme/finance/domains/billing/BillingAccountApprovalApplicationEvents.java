package com.acme.finance.domains.billing;

import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class BillingAccountApprovalApplicationEvents {
    private final CustomerServiceGrpc.CustomerServiceStub customersCustomerService;

    private final DocumentQueryServiceGrpc.DocumentQueryServiceStub documentsDocumentQueryService;

    public BillingAccountApprovalApplicationEvents(CustomerServiceGrpc.CustomerServiceStub customersCustomerService, DocumentQueryServiceGrpc.DocumentQueryServiceStub documentsDocumentQueryService) {
        this.customersCustomerService = customersCustomerService;
        this.documentsDocumentQueryService = documentsDocumentQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(BillingAccountApprovalApplication.ApproveBillingAccountStage1Requested event) {
        event.context().run(() -> {
            try {
                customersCustomerService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .searchCustomers(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(BillingAccountApprovalApplication.ListPendingBillingAccountsStage1Requested event) {
        event.context().run(() -> {
            try {
                documentsDocumentQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .browseDocuments(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
