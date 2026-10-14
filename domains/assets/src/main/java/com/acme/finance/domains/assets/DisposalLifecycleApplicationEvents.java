package com.acme.finance.domains.assets;

import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class DisposalLifecycleApplicationEvents {
    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncService;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceStub ledgerPostingBatchValidationService;

    public DisposalLifecycleApplicationEvents(DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncService, PostingBatchValidationServiceGrpc.PostingBatchValidationServiceStub ledgerPostingBatchValidationService) {
        this.assetsDisposalSyncService = assetsDisposalSyncService;
        this.ledgerPostingBatchValidationService = ledgerPostingBatchValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(DisposalLifecycleApplication.ReinstateDisposalStage0Requested event) {
        event.context().run(() -> {
            try {
                ledgerPostingBatchValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .checkPostingBatchEligibility(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(DisposalLifecycleApplication.TerminateDisposalStage0Requested event) {
        event.context().run(() -> {
            try {
                assetsDisposalSyncService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listDisposalConflicts(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
