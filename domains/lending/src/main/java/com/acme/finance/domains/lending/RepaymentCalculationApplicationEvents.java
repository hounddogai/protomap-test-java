package com.acme.finance.domains.lending;

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
public class RepaymentCalculationApplicationEvents {
    private final BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountService;

    public RepaymentCalculationApplicationEvents(BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountService) {
        this.bankingBankAccountService = bankingBankAccountService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RepaymentCalculationApplication.ExplainRepaymentCalculationStage2Requested event) {
        event.context().run(() -> {
            try {
                bankingBankAccountService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .createBankAccount(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
