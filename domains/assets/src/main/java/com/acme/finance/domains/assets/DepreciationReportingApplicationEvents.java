package com.acme.finance.domains.assets;

import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class DepreciationReportingApplicationEvents {
    private final DepreciationValidationServiceGrpc.DepreciationValidationServiceStub assetsDepreciationValidationService;

    public DepreciationReportingApplicationEvents(DepreciationValidationServiceGrpc.DepreciationValidationServiceStub assetsDepreciationValidationService) {
        this.assetsDepreciationValidationService = assetsDepreciationValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(DepreciationReportingApplication.ShareDepreciationReportStage1Requested event) {
        event.context().run(() -> {
            try {
                assetsDepreciationValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .scoreDepreciation(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
