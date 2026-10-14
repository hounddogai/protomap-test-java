package com.acme.finance.domains.billing;

import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class PaymentTermValidationApplicationEvents {
    private final PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceStub billingPaymentTermCalculationService;

    public PaymentTermValidationApplicationEvents(PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceStub billingPaymentTermCalculationService) {
        this.billingPaymentTermCalculationService = billingPaymentTermCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PaymentTermValidationApplication.ScorePaymentTermStage0Requested event) {
        event.context().run(() -> {
            try {
                billingPaymentTermCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .recalculatePaymentTerms(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
