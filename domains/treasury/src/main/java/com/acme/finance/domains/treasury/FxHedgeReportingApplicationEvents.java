package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.CashPositionQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class FxHedgeReportingApplicationEvents {
    private final CashPositionQueryServiceGrpc.CashPositionQueryServiceStub treasuryCashPositionQueryService;

    public FxHedgeReportingApplicationEvents(CashPositionQueryServiceGrpc.CashPositionQueryServiceStub treasuryCashPositionQueryService) {
        this.treasuryCashPositionQueryService = treasuryCashPositionQueryService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(FxHedgeReportingApplication.DeleteFxHedgeReportStage0Requested event) {
        event.context().run(() -> {
            try {
                treasuryCashPositionQueryService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .resolveCashPositions(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
