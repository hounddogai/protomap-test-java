package com.acme.finance.domains.contracts;

import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ContractClauseApprovalApplicationEvents {
    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceStub customersConsentRecordCalculationService;

    public ContractClauseApprovalApplicationEvents(ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceStub customersConsentRecordCalculationService) {
        this.customersConsentRecordCalculationService = customersConsentRecordCalculationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ContractClauseApprovalApplication.ApproveContractClauseStage1Requested event) {
        event.context().run(() -> {
            try {
                customersConsentRecordCalculationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .calculateConsentRecord(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
