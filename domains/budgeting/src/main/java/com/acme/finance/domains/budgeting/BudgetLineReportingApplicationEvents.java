package com.acme.finance.domains.budgeting;

import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class BudgetLineReportingApplicationEvents {
    private final UserQueryServiceGrpc.UserQueryServiceStub identityUserQueryService;

    public BudgetLineReportingApplicationEvents(UserQueryServiceGrpc.UserQueryServiceStub identityUserQueryService) {
        this.identityUserQueryService = identityUserQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(BudgetLineReportingApplication.ListBudgetLineReportsStage0Requested event) {
        event.context().run(() -> {
            try {
                identityUserQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .browseUsers(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
