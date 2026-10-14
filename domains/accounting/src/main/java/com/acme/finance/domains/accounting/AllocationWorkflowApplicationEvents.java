package com.acme.finance.domains.accounting;

import com.acme.assets.v1.FixedAssetWorkflowServiceGrpc;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class AllocationWorkflowApplicationEvents {
    private final FixedAssetWorkflowServiceGrpc.FixedAssetWorkflowServiceStub assetsFixedAssetWorkflowService;

    private final DocumentQueryServiceGrpc.DocumentQueryServiceStub documentsDocumentQueryService;

    public AllocationWorkflowApplicationEvents(FixedAssetWorkflowServiceGrpc.FixedAssetWorkflowServiceStub assetsFixedAssetWorkflowService, DocumentQueryServiceGrpc.DocumentQueryServiceStub documentsDocumentQueryService) {
        this.assetsFixedAssetWorkflowService = assetsFixedAssetWorkflowService;
        this.documentsDocumentQueryService = documentsDocumentQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(AllocationWorkflowApplication.StartAllocationWorkflowStage1Requested event) {
        event.context().run(() -> {
            try {
                assetsFixedAssetWorkflowService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .resumeFixedAssetWorkflow(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(AllocationWorkflowApplication.ResumeAllocationWorkflowStage1Requested event) {
        event.context().run(() -> {
            try {
                documentsDocumentQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .summarizeDocuments(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
