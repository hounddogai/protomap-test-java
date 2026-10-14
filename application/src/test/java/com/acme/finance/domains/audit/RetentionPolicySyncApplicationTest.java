package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.GetRetentionPolicySyncStateRequest;
import com.acme.audit.v1.GetRetentionPolicySyncStateResponse;
import com.acme.audit.v1.ListRetentionPolicyConflictsRequest;
import com.acme.audit.v1.ListRetentionPolicyConflictsResponse;
import com.acme.audit.v1.PullRetentionPoliciesRequest;
import com.acme.audit.v1.PullRetentionPoliciesResponse;
import com.acme.audit.v1.PushRetentionPolicyRequest;
import com.acme.audit.v1.PushRetentionPolicyResponse;
import com.acme.audit.v1.ReconcileRetentionPoliciesRequest;
import com.acme.audit.v1.ReconcileRetentionPoliciesResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SyncRetentionPoliciesRequest;
import com.acme.audit.v1.SyncRetentionPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class RetentionPolicySyncApplicationTest {
    @Test
    void syncRetentionPoliciesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new RetentionPolicySyncApplication(
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newFutureStub(channel),
                repository);
        var request = ProbeSupport.sample(SyncRetentionPoliciesRequest.getDefaultInstance());
        var response = application.syncRetentionPolicies(request);
        var sent0 = channel.request("acme.ledger.v1.LedgerAccountNotificationService/SendLedgerAccountTestNotice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "retention_policy", "owner", "email"),
                ProbeSupport.value(sent0, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountNotificationService/SendLedgerAccountTestNotice"), "ledger_account", "audit", "created_at"),
                ProbeSupport.value(response, "retention_policy", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "retention_policy_id"),
                ProbeSupport.value(response, "retention_policy", "retention_policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "retention_policy", "priority"),
                ProbeSupport.value(response, "retention_policy", "priority"));
    }

    @Test
    void streamRetentionPolicyChangesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new RetentionPolicySyncApplication(
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newFutureStub(channel),
                repository);
        var request = ProbeSupport.sample(StreamRetentionPolicyChangesRequest.getDefaultInstance());
        var response = application.streamRetentionPolicyChanges(request);
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "priority"),
                ProbeSupport.value(response, "retention_policy", "priority"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "ack", "sequence"),
                ProbeSupport.value(response, "sequence"));
        var endpoint = new RetentionPolicySyncEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamRetentionPolicyChangesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamRetentionPolicyChangesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.streamRetentionPolicyChanges(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "value", "completed"), signals);
    }
}
