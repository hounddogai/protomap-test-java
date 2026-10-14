package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class DeductionReportingApplicationEvents {
    private final DeductionValidationServiceGrpc.DeductionValidationServiceStub payrollDeductionValidationService;

    public DeductionReportingApplicationEvents(DeductionValidationServiceGrpc.DeductionValidationServiceStub payrollDeductionValidationService) {
        this.payrollDeductionValidationService = payrollDeductionValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(DeductionReportingApplication.ScheduleDeductionReportStage1Requested event) {
        event.context().run(() -> {
            try {
                payrollDeductionValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .normalizeDeduction(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
