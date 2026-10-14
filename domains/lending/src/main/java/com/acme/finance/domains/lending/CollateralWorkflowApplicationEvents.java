package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.payments.v1.PaymentCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class CollateralWorkflowApplicationEvents {
    private final PaymentCalculationServiceGrpc.PaymentCalculationServiceStub paymentsPaymentCalculationService;

    public CollateralWorkflowApplicationEvents(PaymentCalculationServiceGrpc.PaymentCalculationServiceStub paymentsPaymentCalculationService) {
        this.paymentsPaymentCalculationService = paymentsPaymentCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CollateralWorkflowApplication.ResumeCollateralWorkflowStage1Requested event) {
        event.context().run(() -> {
            try {
                paymentsPaymentCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .calculatePayment(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
