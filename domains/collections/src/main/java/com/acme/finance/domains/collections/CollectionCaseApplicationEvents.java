package com.acme.finance.domains.collections;

import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class CollectionCaseApplicationEvents {
    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceStub collectionsWriteOffValidationService;

    public CollectionCaseApplicationEvents(WriteOffValidationServiceGrpc.WriteOffValidationServiceStub collectionsWriteOffValidationService) {
        this.collectionsWriteOffValidationService = collectionsWriteOffValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(CollectionCaseApplication.GetCollectionCaseStage1Requested event) {
        event.context().run(() -> {
            try {
                collectionsWriteOffValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .previewWriteOff(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
