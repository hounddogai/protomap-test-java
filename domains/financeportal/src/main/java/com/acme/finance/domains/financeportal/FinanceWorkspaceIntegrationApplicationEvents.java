package com.acme.finance.domains.financeportal;

import com.acme.datahub.v1.WorkspaceServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class FinanceWorkspaceIntegrationApplicationEvents {
    private final WorkspaceServiceGrpc.WorkspaceServiceStub datahubWorkspaceService;

    public FinanceWorkspaceIntegrationApplicationEvents(WorkspaceServiceGrpc.WorkspaceServiceStub datahubWorkspaceService) {
        this.datahubWorkspaceService = datahubWorkspaceService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(FinanceWorkspaceIntegrationApplication.RelayFinanceWorkspaceMessagesStage0Requested event) {
        event.context().run(() -> {
            try {
                datahubWorkspaceService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .batchGetWorkspaces(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
