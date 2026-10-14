package com.acme.finance.domains.invoicing;

import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.BankTransferWorkflowServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class InvoiceBatchApprovalApplicationEvents {
    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceStub customersPreferenceCalculationService;

    private final BankTransferWorkflowServiceGrpc.BankTransferWorkflowServiceStub treasuryBankTransferWorkflowService;

    public InvoiceBatchApprovalApplicationEvents(PreferenceCalculationServiceGrpc.PreferenceCalculationServiceStub customersPreferenceCalculationService, BankTransferWorkflowServiceGrpc.BankTransferWorkflowServiceStub treasuryBankTransferWorkflowService) {
        this.customersPreferenceCalculationService = customersPreferenceCalculationService;
        this.treasuryBankTransferWorkflowService = treasuryBankTransferWorkflowService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceBatchApprovalApplication.EscalateInvoiceBatchStage1Requested event) {
        event.context().run(() -> {
            try {
                customersPreferenceCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .calculatePreference(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }

    @Async("eventExecutor")
    @EventListener
    public void on(InvoiceBatchApprovalApplication.DelegateInvoiceBatchApprovalStage2Requested event) {
        event.context().run(() -> {
            try {
                treasuryBankTransferWorkflowService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .cancelBankTransferWorkflow(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
