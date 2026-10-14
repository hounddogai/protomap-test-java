package com.acme.finance.domains.billing;

import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class InvoiceApprovalApplicationEvents {
    private final BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceStub billingBillingAccountApprovalService;

    private final UserQueryServiceGrpc.UserQueryServiceStub identityUserQueryService;

    public InvoiceApprovalApplicationEvents(BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceStub billingBillingAccountApprovalService, UserQueryServiceGrpc.UserQueryServiceStub identityUserQueryService) {
        this.billingBillingAccountApprovalService = billingBillingAccountApprovalService;
        this.identityUserQueryService = identityUserQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceApprovalApplication.ApproveInvoiceStage2Requested event) {
        event.context().run(() -> {
            try {
                billingBillingAccountApprovalService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .delegateBillingAccountApproval(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceApprovalApplication.EscalateInvoiceStage2Requested event) {
        event.context().run(() -> {
            try {
                identityUserQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .findUsers(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
