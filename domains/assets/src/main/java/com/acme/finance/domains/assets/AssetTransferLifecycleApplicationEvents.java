package com.acme.finance.domains.assets;

import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class AssetTransferLifecycleApplicationEvents {
    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncService;

    public AssetTransferLifecycleApplicationEvents(DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncService) {
        this.assetsDisposalSyncService = assetsDisposalSyncService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(AssetTransferLifecycleApplication.TerminateAssetTransferStage1Requested event) {
        event.context().run(() -> {
            try {
                assetsDisposalSyncService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .pushDisposal(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
