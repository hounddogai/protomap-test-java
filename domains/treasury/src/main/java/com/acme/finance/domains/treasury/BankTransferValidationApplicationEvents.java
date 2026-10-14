package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.maintenance.v1.WorkOrderCalculationServiceGrpc;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class BankTransferValidationApplicationEvents {
    private final WorkOrderCalculationServiceGrpc.WorkOrderCalculationServiceStub maintenanceWorkOrderCalculationService;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceStub metricsMetricSeriesQueryService;

    public BankTransferValidationApplicationEvents(WorkOrderCalculationServiceGrpc.WorkOrderCalculationServiceStub maintenanceWorkOrderCalculationService, MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceStub metricsMetricSeriesQueryService) {
        this.maintenanceWorkOrderCalculationService = maintenanceWorkOrderCalculationService;
        this.metricsMetricSeriesQueryService = metricsMetricSeriesQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(BankTransferValidationApplication.ExplainBankTransferDecisionStage0Requested event) {
        event.context().run(() -> {
            try {
                maintenanceWorkOrderCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .simulateWorkOrder(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(BankTransferValidationApplication.ScoreBankTransferStage1Requested event) {
        event.context().run(() -> {
            try {
                metricsMetricSeriesQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .browseMetricSerieses(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
