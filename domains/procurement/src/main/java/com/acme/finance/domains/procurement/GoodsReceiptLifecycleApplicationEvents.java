package com.acme.finance.domains.procurement;

import com.acme.compliance.v1.SanctionApprovalServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class GoodsReceiptLifecycleApplicationEvents {
    private final SanctionApprovalServiceGrpc.SanctionApprovalServiceStub complianceSanctionApprovalService;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionService;

    public GoodsReceiptLifecycleApplicationEvents(SanctionApprovalServiceGrpc.SanctionApprovalServiceStub complianceSanctionApprovalService, PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionService) {
        this.complianceSanctionApprovalService = complianceSanctionApprovalService;
        this.procurementPurchaseRequisitionService = procurementPurchaseRequisitionService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(GoodsReceiptLifecycleApplication.ReinstateGoodsReceiptStage1Requested event) {
        event.context().run(() -> {
            try {
                complianceSanctionApprovalService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .rejectSanction(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(GoodsReceiptLifecycleApplication.ExpireGoodsReceiptStage1Requested event) {
        event.context().run(() -> {
            try {
                procurementPurchaseRequisitionService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .updatePurchaseRequisition(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
