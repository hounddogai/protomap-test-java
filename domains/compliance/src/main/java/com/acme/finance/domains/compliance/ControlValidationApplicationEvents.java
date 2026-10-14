package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.ControlCalculationServiceGrpc;
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
public class ControlValidationApplicationEvents {
    private final ControlCalculationServiceGrpc.ControlCalculationServiceStub complianceControlCalculationService;

    private final VenueQueryServiceGrpc.VenueQueryServiceStub venuesVenueQueryService;

    public ControlValidationApplicationEvents(ControlCalculationServiceGrpc.ControlCalculationServiceStub complianceControlCalculationService, VenueQueryServiceGrpc.VenueQueryServiceStub venuesVenueQueryService) {
        this.complianceControlCalculationService = complianceControlCalculationService;
        this.venuesVenueQueryService = venuesVenueQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ControlValidationApplication.ValidateControlStage0Requested event) {
        event.context().run(() -> {
            try {
                venuesVenueQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .findVenues(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ControlValidationApplication.GetControlRulesStage0Requested event) {
        event.context().run(() -> {
            try {
                complianceControlCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .getControlRates(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
