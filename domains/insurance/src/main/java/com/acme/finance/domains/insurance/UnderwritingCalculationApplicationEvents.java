package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class UnderwritingCalculationApplicationEvents {
    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceStub metricsMetricSeriesQueryService;

    public UnderwritingCalculationApplicationEvents(MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceStub metricsMetricSeriesQueryService) {
        this.metricsMetricSeriesQueryService = metricsMetricSeriesQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(UnderwritingCalculationApplication.SimulateUnderwritingStage3Requested event) {
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
