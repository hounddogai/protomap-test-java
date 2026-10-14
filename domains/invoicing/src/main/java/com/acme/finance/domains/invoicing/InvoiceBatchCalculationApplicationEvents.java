package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.sponsorships.v1.SponsorCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class InvoiceBatchCalculationApplicationEvents {
    private final SponsorCalculationServiceGrpc.SponsorCalculationServiceStub sponsorshipsSponsorCalculationService;

    public InvoiceBatchCalculationApplicationEvents(SponsorCalculationServiceGrpc.SponsorCalculationServiceStub sponsorshipsSponsorCalculationService) {
        this.sponsorshipsSponsorCalculationService = sponsorshipsSponsorCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceBatchCalculationApplication.QuoteInvoiceBatchStage1Requested event) {
        event.context().run(() -> {
            try {
                sponsorshipsSponsorCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .calculateSponsor(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
