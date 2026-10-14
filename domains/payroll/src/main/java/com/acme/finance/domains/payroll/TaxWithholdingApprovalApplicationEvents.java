package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class TaxWithholdingApprovalApplicationEvents {
    private final UserServiceGrpc.UserServiceStub identityUserService;

    public TaxWithholdingApprovalApplicationEvents(UserServiceGrpc.UserServiceStub identityUserService) {
        this.identityUserService = identityUserService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(TaxWithholdingApprovalApplication.ApproveTaxWithholdingStage1Requested event) {
        event.context().run(() -> {
            try {
                identityUserService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .getUser(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
