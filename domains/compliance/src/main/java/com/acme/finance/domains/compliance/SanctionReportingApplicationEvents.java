package com.acme.finance.domains.compliance;

import com.acme.finance.platform.RpcSupport;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class SanctionReportingApplicationEvents {
    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceStub searchSearchIndexValidationService;

    public SanctionReportingApplicationEvents(SearchIndexValidationServiceGrpc.SearchIndexValidationServiceStub searchSearchIndexValidationService) {
        this.searchSearchIndexValidationService = searchSearchIndexValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(SanctionReportingApplication.FetchSanctionReportStage0Requested event) {
        event.context().run(() -> {
            try {
                searchSearchIndexValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .checkSearchIndexEligibility(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
