package com.acme.finance.domains.assets;

import com.acme.finance.platform.RpcSupport;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class DisposalReportingApplicationEvents {
    private final VenueValidationServiceGrpc.VenueValidationServiceStub venuesVenueValidationService;

    public DisposalReportingApplicationEvents(VenueValidationServiceGrpc.VenueValidationServiceStub venuesVenueValidationService) {
        this.venuesVenueValidationService = venuesVenueValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(DisposalReportingApplication.ShareDisposalReportStage0Requested event) {
        event.context().run(() -> {
            try {
                venuesVenueValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .validateVenue(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(DisposalReportingApplication.DeleteDisposalReportStage0Requested event) {
        event.context().run(() -> {
            try {
                venuesVenueValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .explainVenueDecision(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
