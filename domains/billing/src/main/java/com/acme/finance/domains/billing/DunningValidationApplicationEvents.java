package com.acme.finance.domains.billing;

import com.acme.billing.v1.PaymentTermValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class DunningValidationApplicationEvents {
    private final PaymentTermValidationServiceGrpc.PaymentTermValidationServiceStub billingPaymentTermValidationService;

    public DunningValidationApplicationEvents(PaymentTermValidationServiceGrpc.PaymentTermValidationServiceStub billingPaymentTermValidationService) {
        this.billingPaymentTermValidationService = billingPaymentTermValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(DunningValidationApplication.GetDunningRulesStage0Requested event) {
        event.context().run(() -> {
            try {
                billingPaymentTermValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .explainPaymentTermDecision(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
