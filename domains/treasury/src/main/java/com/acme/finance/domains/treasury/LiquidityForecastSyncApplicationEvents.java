package com.acme.finance.domains.treasury;

import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class LiquidityForecastSyncApplicationEvents {
    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceStub authorizationAccessPolicyQueryService;

    public LiquidityForecastSyncApplicationEvents(AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceStub authorizationAccessPolicyQueryService) {
        this.authorizationAccessPolicyQueryService = authorizationAccessPolicyQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(LiquidityForecastSyncApplication.PullLiquidityForecastsStage2Requested event) {
        event.context().run(() -> {
            try {
                authorizationAccessPolicyQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .countAccessPolicies(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
