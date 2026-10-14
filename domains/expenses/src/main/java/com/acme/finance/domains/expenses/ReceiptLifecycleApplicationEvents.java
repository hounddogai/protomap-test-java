package com.acme.finance.domains.expenses;

import com.acme.dispatch.v1.DispatchOrderQueryServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class ReceiptLifecycleApplicationEvents {
    private final DispatchOrderQueryServiceGrpc.DispatchOrderQueryServiceStub dispatchDispatchOrderQueryService;

    public ReceiptLifecycleApplicationEvents(DispatchOrderQueryServiceGrpc.DispatchOrderQueryServiceStub dispatchDispatchOrderQueryService) {
        this.dispatchDispatchOrderQueryService = dispatchDispatchOrderQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(ReceiptLifecycleApplication.ExpireReceiptStage0Requested event) {
        event.context().run(() -> {
            try {
                dispatchDispatchOrderQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .browseDispatchOrders(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
