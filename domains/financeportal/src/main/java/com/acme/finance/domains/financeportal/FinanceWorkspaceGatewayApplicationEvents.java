package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.GoodsReceiptWorkflowServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class FinanceWorkspaceGatewayApplicationEvents {
    private final GoodsReceiptWorkflowServiceGrpc.GoodsReceiptWorkflowServiceStub procurementGoodsReceiptWorkflowService;

    public FinanceWorkspaceGatewayApplicationEvents(GoodsReceiptWorkflowServiceGrpc.GoodsReceiptWorkflowServiceStub procurementGoodsReceiptWorkflowService) {
        this.procurementGoodsReceiptWorkflowService = procurementGoodsReceiptWorkflowService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(FinanceWorkspaceGatewayApplication.ComposeFinanceWorkspacePageStage2Requested event) {
        event.context().run(() -> {
            try {
                procurementGoodsReceiptWorkflowService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .signalGoodsReceiptWorkflow(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
