package com.acme.finance.domains.budgeting;

import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ForecastSyncApplicationEvents {
    private final SupplierQueryServiceGrpc.SupplierQueryServiceStub suppliersSupplierQueryService;

    public ForecastSyncApplicationEvents(SupplierQueryServiceGrpc.SupplierQueryServiceStub suppliersSupplierQueryService) {
        this.suppliersSupplierQueryService = suppliersSupplierQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ForecastSyncApplication.ResolveForecastConflictStage1Requested event) {
        event.context().run(() -> {
            try {
                suppliersSupplierQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .findSuppliers(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
