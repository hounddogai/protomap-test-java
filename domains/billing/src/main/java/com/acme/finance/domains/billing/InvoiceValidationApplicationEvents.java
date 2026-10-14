package com.acme.finance.domains.billing;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class InvoiceValidationApplicationEvents {
    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceStub payrollPayStubCalculationService;

    public InvoiceValidationApplicationEvents(PayStubCalculationServiceGrpc.PayStubCalculationServiceStub payrollPayStubCalculationService) {
        this.payrollPayStubCalculationService = payrollPayStubCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceValidationApplication.ScoreInvoiceStage1Requested event) {
        event.context().run(() -> {
            try {
                payrollPayStubCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .estimatePayStub(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
