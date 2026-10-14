package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ApprovalInboxQueryApplicationEvents {
    private final TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceStub payrollTaxWithholdingReportingService;

    public ApprovalInboxQueryApplicationEvents(TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceStub payrollTaxWithholdingReportingService) {
        this.payrollTaxWithholdingReportingService = payrollTaxWithholdingReportingService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ApprovalInboxQueryApplication.PeekApprovalInboxStage0Requested event) {
        event.context().run(() -> {
            try {
                payrollTaxWithholdingReportingService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .generateTaxWithholdingReport(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
