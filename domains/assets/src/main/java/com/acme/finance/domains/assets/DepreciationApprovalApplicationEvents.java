package com.acme.finance.domains.assets;

import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class DepreciationApprovalApplicationEvents {
    private final FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceStub assetsFixedAssetApprovalService;

    public DepreciationApprovalApplicationEvents(FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceStub assetsFixedAssetApprovalService) {
        this.assetsFixedAssetApprovalService = assetsFixedAssetApprovalService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(DepreciationApprovalApplication.SubmitDepreciationStage0Requested event) {
        event.context().run(() -> {
            try {
                assetsFixedAssetApprovalService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .rejectFixedAsset(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
