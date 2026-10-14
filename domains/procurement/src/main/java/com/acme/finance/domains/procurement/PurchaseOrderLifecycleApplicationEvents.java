package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class PurchaseOrderLifecycleApplicationEvents {
    private final PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceStub procurementPurchaseOrderValidationService;

    public PurchaseOrderLifecycleApplicationEvents(PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceStub procurementPurchaseOrderValidationService) {
        this.procurementPurchaseOrderValidationService = procurementPurchaseOrderValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PurchaseOrderLifecycleApplication.ActivatePurchaseOrderStage1Requested event) {
        event.context().run(() -> {
            try {
                procurementPurchaseOrderValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .previewPurchaseOrder(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
