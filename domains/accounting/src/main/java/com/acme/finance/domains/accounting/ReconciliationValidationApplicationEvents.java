package com.acme.finance.domains.accounting;

import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ReconciliationValidationApplicationEvents {
    private final DisposalCalculationServiceGrpc.DisposalCalculationServiceStub assetsDisposalCalculationService;

    public ReconciliationValidationApplicationEvents(DisposalCalculationServiceGrpc.DisposalCalculationServiceStub assetsDisposalCalculationService) {
        this.assetsDisposalCalculationService = assetsDisposalCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ReconciliationValidationApplication.NormalizeReconciliationStage2Requested event) {
        event.context().run(() -> {
            try {
                assetsDisposalCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .quoteDisposal(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
