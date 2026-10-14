package com.acme.finance.domains.collections;

import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class PaymentPromiseApprovalApplicationEvents {
    private final MandateApprovalServiceGrpc.MandateApprovalServiceStub bankingMandateApprovalService;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceStub collectionsWriteOffValidationService;

    public PaymentPromiseApprovalApplicationEvents(MandateApprovalServiceGrpc.MandateApprovalServiceStub bankingMandateApprovalService, WriteOffValidationServiceGrpc.WriteOffValidationServiceStub collectionsWriteOffValidationService) {
        this.bankingMandateApprovalService = bankingMandateApprovalService;
        this.collectionsWriteOffValidationService = collectionsWriteOffValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PaymentPromiseApprovalApplication.ApprovePaymentPromiseStage0Requested event) {
        event.context().run(() -> {
            try {
                bankingMandateApprovalService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listPendingMandates(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PaymentPromiseApprovalApplication.RejectPaymentPromiseStage0Requested event) {
        event.context().run(() -> {
            try {
                collectionsWriteOffValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .normalizeWriteOff(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
