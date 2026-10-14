package com.acme.finance.domains.contracts;

import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ContractApplicationEvents {
    private final FeatureFlagServiceGrpc.FeatureFlagServiceStub featureflagsFeatureFlagService;

    public ContractApplicationEvents(FeatureFlagServiceGrpc.FeatureFlagServiceStub featureflagsFeatureFlagService) {
        this.featureflagsFeatureFlagService = featureflagsFeatureFlagService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ContractApplication.UpdateContractStage2Requested event) {
        event.context().run(() -> {
            try {
                featureflagsFeatureFlagService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .batchGetFeatureFlags(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
