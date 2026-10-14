package com.acme.finance.domains.suppliers;

import com.acme.finance.platform.RpcSupport;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class SupplierContractApprovalApplicationEvents {
    private final MetricSeriesServiceGrpc.MetricSeriesServiceStub metricsMetricSeriesService;

    public SupplierContractApprovalApplicationEvents(MetricSeriesServiceGrpc.MetricSeriesServiceStub metricsMetricSeriesService) {
        this.metricsMetricSeriesService = metricsMetricSeriesService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(SupplierContractApprovalApplication.GetSupplierContractApprovalStatusStage0Requested event) {
        event.context().run(() -> {
            try {
                metricsMetricSeriesService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listMetricSerieses(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
