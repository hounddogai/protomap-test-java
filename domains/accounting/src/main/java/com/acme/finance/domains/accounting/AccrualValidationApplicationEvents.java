package com.acme.finance.domains.accounting;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class AccrualValidationApplicationEvents {
    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceStub ledgerFiscalPeriodNotificationService;

    public AccrualValidationApplicationEvents(FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceStub ledgerFiscalPeriodNotificationService) {
        this.ledgerFiscalPeriodNotificationService = ledgerFiscalPeriodNotificationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(AccrualValidationApplication.ExplainAccrualDecisionStage2Requested event) {
        event.context().run(() -> {
            try {
                ledgerFiscalPeriodNotificationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listFiscalPeriodSubscriptions(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
