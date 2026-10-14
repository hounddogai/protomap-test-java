package com.acme.finance.domains.financeportal;

import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.CloseChecklistIntegrationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ApprovalInboxNotificationApplicationEvents {
    private final CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceStub financeportalCloseChecklistIntegrationService;

    public ApprovalInboxNotificationApplicationEvents(CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceStub financeportalCloseChecklistIntegrationService) {
        this.financeportalCloseChecklistIntegrationService = financeportalCloseChecklistIntegrationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ApprovalInboxNotificationApplication.GetApprovalInboxPreferencesStage0Requested event) {
        event.context().run(() -> {
            try {
                financeportalCloseChecklistIntegrationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listCloseChecklistSources(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
