package com.acme.finance.domains.risk;

import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class RiskRegisterWorkflowApplicationEvents {
    private final CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceStub collectionsCollectionCaseApprovalService;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceStub customersPreferenceCalculationService;

    public RiskRegisterWorkflowApplicationEvents(CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceStub collectionsCollectionCaseApprovalService, PreferenceCalculationServiceGrpc.PreferenceCalculationServiceStub customersPreferenceCalculationService) {
        this.collectionsCollectionCaseApprovalService = collectionsCollectionCaseApprovalService;
        this.customersPreferenceCalculationService = customersPreferenceCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RiskRegisterWorkflowApplication.CancelRiskRegisterWorkflowStage0Requested event) {
        event.context().run(() -> {
            try {
                collectionsCollectionCaseApprovalService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .rejectCollectionCase(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RiskRegisterWorkflowApplication.PauseRiskRegisterWorkflowStage2Requested event) {
        event.context().run(() -> {
            try {
                customersPreferenceCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .comparePreferenceOptions(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
