package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class InvoiceBatchApplicationEvents {
    private final AuditTrailQueryServiceGrpc.AuditTrailQueryServiceStub auditAuditTrailQueryService;

    private final BankTransferCalculationServiceGrpc.BankTransferCalculationServiceStub treasuryBankTransferCalculationService;

    public InvoiceBatchApplicationEvents(AuditTrailQueryServiceGrpc.AuditTrailQueryServiceStub auditAuditTrailQueryService, BankTransferCalculationServiceGrpc.BankTransferCalculationServiceStub treasuryBankTransferCalculationService) {
        this.auditAuditTrailQueryService = auditAuditTrailQueryService;
        this.treasuryBankTransferCalculationService = treasuryBankTransferCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceBatchApplication.DeleteInvoiceBatchStage2Requested event) {
        event.context().run(() -> {
            try {
                auditAuditTrailQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .suggestAuditTrails(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceBatchApplication.WatchInvoiceBatchesStage1Requested event) {
        event.context().run(() -> {
            try {
                treasuryBankTransferCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .recalculateBankTransfers(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
