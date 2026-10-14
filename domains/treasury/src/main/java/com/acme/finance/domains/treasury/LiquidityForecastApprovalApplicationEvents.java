package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.treasury.v1.CashPositionLifecycleServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class LiquidityForecastApprovalApplicationEvents {
    private final UserServiceGrpc.UserServiceStub identityUserService;

    private final CashPositionLifecycleServiceGrpc.CashPositionLifecycleServiceStub treasuryCashPositionLifecycleService;

    public LiquidityForecastApprovalApplicationEvents(UserServiceGrpc.UserServiceStub identityUserService, CashPositionLifecycleServiceGrpc.CashPositionLifecycleServiceStub treasuryCashPositionLifecycleService) {
        this.identityUserService = identityUserService;
        this.treasuryCashPositionLifecycleService = treasuryCashPositionLifecycleService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(LiquidityForecastApprovalApplication.SubmitLiquidityForecastStage0Requested event) {
        event.context().run(() -> {
            try {
                identityUserService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .createUser(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(LiquidityForecastApprovalApplication.RecallLiquidityForecastStage0Requested event) {
        event.context().run(() -> {
            try {
                treasuryCashPositionLifecycleService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .getCashPositionLifecycle(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
