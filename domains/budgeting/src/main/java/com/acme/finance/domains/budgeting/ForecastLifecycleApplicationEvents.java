package com.acme.finance.domains.budgeting;

import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ForecastLifecycleApplicationEvents {
    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceStub suppliersSupplierRatingValidationService;

    public ForecastLifecycleApplicationEvents(SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceStub suppliersSupplierRatingValidationService) {
        this.suppliersSupplierRatingValidationService = suppliersSupplierRatingValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ForecastLifecycleApplication.RenewForecastStage0Requested event) {
        event.context().run(() -> {
            try {
                suppliersSupplierRatingValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .validateSupplierRating(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
