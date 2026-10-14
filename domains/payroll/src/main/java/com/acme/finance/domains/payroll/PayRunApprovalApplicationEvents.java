package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Consumes commands on the background event executor.
 */
@Component
public class PayRunApprovalApplicationEvents {
    private final NotificationValidationServiceGrpc.NotificationValidationServiceStub notificationsNotificationValidationService;

    public PayRunApprovalApplicationEvents(NotificationValidationServiceGrpc.NotificationValidationServiceStub notificationsNotificationValidationService) {
        this.notificationsNotificationValidationService = notificationsNotificationValidationService;
    }

    @Async("eventExecutor")
    @EventListener
    public void on(PayRunApprovalApplication.RejectPayRunStage0Requested event) {
        event.context().run(() -> {
            try {
                notificationsNotificationValidationService.withDeadlineAfter(8, TimeUnit.SECONDS)
                        .previewNotification(event.request(), RpcSupport.completing(event.completion()));
            } catch (RuntimeException failure) {
                event.completion().completeExceptionally(failure);
            }
        });
    }
}
