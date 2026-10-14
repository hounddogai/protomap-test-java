package com.acme.finance.domains.procurement;

import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class RfqReportingApplicationEvents {
    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceStub customersHouseholdQueryService;

    private final RfqCalculationServiceGrpc.RfqCalculationServiceStub procurementRfqCalculationService;

    public RfqReportingApplicationEvents(HouseholdQueryServiceGrpc.HouseholdQueryServiceStub customersHouseholdQueryService, RfqCalculationServiceGrpc.RfqCalculationServiceStub procurementRfqCalculationService) {
        this.customersHouseholdQueryService = customersHouseholdQueryService;
        this.procurementRfqCalculationService = procurementRfqCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RfqReportingApplication.GenerateRfqReportStage0Requested event) {
        event.context().run(() -> {
            try {
                customersHouseholdQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .browseHouseholds(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RfqReportingApplication.GenerateRfqReportStage2Requested event) {
        event.context().run(() -> {
            try {
                procurementRfqCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .explainRfqCalculation(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
