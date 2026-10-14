package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class UnderwritingSyncApplicationEvents {
    private final PremiumValidationServiceGrpc.PremiumValidationServiceStub insurancePremiumValidationService;

    public UnderwritingSyncApplicationEvents(PremiumValidationServiceGrpc.PremiumValidationServiceStub insurancePremiumValidationService) {
        this.insurancePremiumValidationService = insurancePremiumValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(UnderwritingSyncApplication.ReconcileUnderwritingsStage3Requested event) {
        event.context().run(() -> {
            try {
                insurancePremiumValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .previewPremium(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
