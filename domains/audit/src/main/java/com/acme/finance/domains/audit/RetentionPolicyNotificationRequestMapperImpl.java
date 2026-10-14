package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.GetRetentionPolicyPreferencesRequest;
import com.acme.audit.v1.GetRetentionPolicyPreferencesResponse;
import com.acme.audit.v1.ListRetentionPolicyConflictsRequest;
import com.acme.audit.v1.ListRetentionPolicyConflictsResponse;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsRequest;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsResponse;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersRequest;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersResponse;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeRequest;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeResponse;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class RetentionPolicyNotificationRequestMapperImpl implements RetentionPolicyNotificationRequestMapper {
    @Override
    public ResolveAuditTrailConflictRequest getRetentionPolicyPreferencesStage0(GetRetentionPolicyPreferencesRequest request) {
        return ResolveAuditTrailConflictRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setRetentionPolicyId(request.getRetentionPolicyId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ResolveAuditTrailConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
    }
}
