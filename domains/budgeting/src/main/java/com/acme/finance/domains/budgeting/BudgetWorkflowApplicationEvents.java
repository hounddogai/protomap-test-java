package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class BudgetWorkflowApplicationEvents {
    private final BudgetLineReportingServiceGrpc.BudgetLineReportingServiceStub budgetingBudgetLineReportingService;

    private final FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceStub featureflagsFeatureFlagQueryService;

    public BudgetWorkflowApplicationEvents(BudgetLineReportingServiceGrpc.BudgetLineReportingServiceStub budgetingBudgetLineReportingService, FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceStub featureflagsFeatureFlagQueryService) {
        this.budgetingBudgetLineReportingService = budgetingBudgetLineReportingService;
        this.featureflagsFeatureFlagQueryService = featureflagsFeatureFlagQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(BudgetWorkflowApplication.AdvanceBudgetWorkflowStage0Requested event) {
        event.context().run(() -> {
            try {
                budgetingBudgetLineReportingService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .countBudgetLineTotals(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(BudgetWorkflowApplication.SignalBudgetWorkflowStage0Requested event) {
        event.context().run(() -> {
            try {
                featureflagsFeatureFlagQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .browseFeatureFlags(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
