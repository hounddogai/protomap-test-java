package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class DeductionWorkflowApplicationEvents {
    private final PayRunValidationServiceGrpc.PayRunValidationServiceStub payrollPayRunValidationService;

    public DeductionWorkflowApplicationEvents(PayRunValidationServiceGrpc.PayRunValidationServiceStub payrollPayRunValidationService) {
        this.payrollPayRunValidationService = payrollPayRunValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(DeductionWorkflowApplication.CancelDeductionWorkflowStage2Requested event) {
        event.context().run(() -> {
            try {
                payrollPayRunValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .previewPayRun(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
