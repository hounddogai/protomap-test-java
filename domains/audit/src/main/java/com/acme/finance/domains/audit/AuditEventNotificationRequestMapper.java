package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.GetAuditEventPreferencesRequest;
import com.acme.audit.v1.GetAuditEventPreferencesResponse;
import com.acme.audit.v1.ListAuditEventSubscriptionsRequest;
import com.acme.audit.v1.ListAuditEventSubscriptionsResponse;
import com.acme.audit.v1.ListAuditTrailConflictsRequest;
import com.acme.audit.v1.ListAuditTrailConflictsResponse;
import com.acme.audit.v1.NotifyAuditEventWatchersRequest;
import com.acme.audit.v1.NotifyAuditEventWatchersResponse;
import com.acme.audit.v1.SendAuditEventTestNoticeRequest;
import com.acme.audit.v1.SendAuditEventTestNoticeResponse;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.audit.v1.UnsubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.UnsubscribeAuditEventUpdatesResponse;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.RpcSupport;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.SearchNotificationsRequest;
import com.acme.notifications.v1.SearchNotificationsResponse;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Maps workflow inputs to downstream contracts. Implementations keep wire construction explicit.
 */
public interface AuditEventNotificationRequestMapper {
    ListAuditTrailConflictsRequest streamAuditEventNoticesStage0(StreamAuditEventNoticesRequest request);
}
