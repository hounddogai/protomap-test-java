package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class LiquidityForecastImportApplicationEvents {
    private final VenueQueryServiceGrpc.VenueQueryServiceStub venuesVenueQueryService;

    public LiquidityForecastImportApplicationEvents(VenueQueryServiceGrpc.VenueQueryServiceStub venuesVenueQueryService) {
        this.venuesVenueQueryService = venuesVenueQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(LiquidityForecastImportApplication.RetryLiquidityForecastImportStage0Requested event) {
        event.context().run(() -> {
            try {
                venuesVenueQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .countVenues(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
