package com.acme.finance.domains.procurement;

import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class PurchaseOrderQueryApplicationEvents {
    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceStub complianceComplianceCaseCalculationService;

    public PurchaseOrderQueryApplicationEvents(ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceStub complianceComplianceCaseCalculationService) {
        this.complianceComplianceCaseCalculationService = complianceComplianceCaseCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PurchaseOrderQueryApplication.CountPurchaseOrdersStage0Requested event) {
        event.context().run(() -> {
            try {
                complianceComplianceCaseCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .quoteComplianceCase(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
