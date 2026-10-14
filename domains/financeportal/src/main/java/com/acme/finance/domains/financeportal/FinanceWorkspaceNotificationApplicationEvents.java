package com.acme.finance.domains.financeportal;

import com.acme.fanportal.v1.FanSessionQueryServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class FinanceWorkspaceNotificationApplicationEvents {
    private final FanSessionQueryServiceGrpc.FanSessionQueryServiceStub fanportalFanSessionQueryService;

    public FinanceWorkspaceNotificationApplicationEvents(FanSessionQueryServiceGrpc.FanSessionQueryServiceStub fanportalFanSessionQueryService) {
        this.fanportalFanSessionQueryService = fanportalFanSessionQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(FinanceWorkspaceNotificationApplication.ListFinanceWorkspaceSubscriptionsStage0Requested event) {
        event.context().run(() -> {
            try {
                fanportalFanSessionQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .countFanSessions(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
