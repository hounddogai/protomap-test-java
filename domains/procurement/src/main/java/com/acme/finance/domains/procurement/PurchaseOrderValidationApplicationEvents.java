package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class PurchaseOrderValidationApplicationEvents {
    private final PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceStub procurementPurchaseRequisitionValidationService;

    public PurchaseOrderValidationApplicationEvents(PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceStub procurementPurchaseRequisitionValidationService) {
        this.procurementPurchaseRequisitionValidationService = procurementPurchaseRequisitionValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PurchaseOrderValidationApplication.NormalizePurchaseOrderStage0Requested event) {
        event.context().run(() -> {
            try {
                procurementPurchaseRequisitionValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .getPurchaseRequisitionRules(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
