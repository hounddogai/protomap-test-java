package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class TaxWithholdingValidationApplicationEvents {
    private final TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceStub payrollTaxWithholdingCalculationService;

    public TaxWithholdingValidationApplicationEvents(TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceStub payrollTaxWithholdingCalculationService) {
        this.payrollTaxWithholdingCalculationService = payrollTaxWithholdingCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(TaxWithholdingValidationApplication.PreviewTaxWithholdingStage0Requested event) {
        event.context().run(() -> {
            try {
                payrollTaxWithholdingCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .explainTaxWithholdingCalculation(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
