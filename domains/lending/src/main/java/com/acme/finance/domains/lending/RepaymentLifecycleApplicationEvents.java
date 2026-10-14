package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class RepaymentLifecycleApplicationEvents {
    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceStub searchSearchIndexQueryService;

    public RepaymentLifecycleApplicationEvents(SearchIndexQueryServiceGrpc.SearchIndexQueryServiceStub searchSearchIndexQueryService) {
        this.searchSearchIndexQueryService = searchSearchIndexQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(RepaymentLifecycleApplication.RenewRepaymentStage1Requested event) {
        event.context().run(() -> {
            try {
                searchSearchIndexQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .resolveSearchIndexes(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
