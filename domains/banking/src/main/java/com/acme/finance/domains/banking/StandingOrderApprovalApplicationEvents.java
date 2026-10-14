package com.acme.finance.domains.banking;

import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class StandingOrderApprovalApplicationEvents {
    private final BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountService;

    public StandingOrderApprovalApplicationEvents(BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountService) {
        this.bankingBankAccountService = bankingBankAccountService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(StandingOrderApprovalApplication.EscalateStandingOrderStage2Requested event) {
        event.context().run(() -> {
            try {
                bankingBankAccountService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .listBankAccounts(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
