package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class PolicyApprovalApplicationEvents {
    private final RfqValidationServiceGrpc.RfqValidationServiceStub procurementRfqValidationService;

    public PolicyApprovalApplicationEvents(RfqValidationServiceGrpc.RfqValidationServiceStub procurementRfqValidationService) {
        this.procurementRfqValidationService = procurementRfqValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PolicyApprovalApplication.RecallPolicyStage0Requested event) {
        event.context().run(() -> {
            try {
                procurementRfqValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .previewRfq(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
