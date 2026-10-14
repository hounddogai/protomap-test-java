package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetWorkflowServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ReconciliationApprovalApplicationEvents {
    private final ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceStub accountingChartOfAccountsApprovalService;

    private final FixedAssetWorkflowServiceGrpc.FixedAssetWorkflowServiceStub assetsFixedAssetWorkflowService;

    public ReconciliationApprovalApplicationEvents(ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceStub accountingChartOfAccountsApprovalService, FixedAssetWorkflowServiceGrpc.FixedAssetWorkflowServiceStub assetsFixedAssetWorkflowService) {
        this.accountingChartOfAccountsApprovalService = accountingChartOfAccountsApprovalService;
        this.assetsFixedAssetWorkflowService = assetsFixedAssetWorkflowService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ReconciliationApprovalApplication.EscalateReconciliationStage1Requested event) {
        event.context().run(() -> {
            try {
                accountingChartOfAccountsApprovalService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .approveChartOfAccounts(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ReconciliationApprovalApplication.DelegateReconciliationApprovalStage0Requested event) {
        event.context().run(() -> {
            try {
                assetsFixedAssetWorkflowService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listFixedAssetWorkflows(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
