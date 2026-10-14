package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.CancelAuditTrailExportRequest;
import com.acme.audit.v1.CancelAuditTrailExportResponse;
import com.acme.audit.v1.GetAuditTrailPreferencesRequest;
import com.acme.audit.v1.GetAuditTrailPreferencesResponse;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.ListAuditTrailSubscriptionsRequest;
import com.acme.audit.v1.ListAuditTrailSubscriptionsResponse;
import com.acme.audit.v1.NotifyAuditTrailWatchersRequest;
import com.acme.audit.v1.NotifyAuditTrailWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.SendAuditTrailTestNoticeRequest;
import com.acme.audit.v1.SendAuditTrailTestNoticeResponse;
import com.acme.audit.v1.StreamAuditTrailNoticesRequest;
import com.acme.audit.v1.StreamAuditTrailNoticesResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesResponse;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class AuditTrailNotificationApplicationTest {
    @Test
    void streamAuditTrailNoticesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new AuditTrailNotificationApplication(
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                repository);
        var request = ProbeSupport.sample(StreamAuditTrailNoticesRequest.getDefaultInstance());
        var response = application.streamAuditTrailNotices(request);
        var sent0 = channel.request("acme.audit.v1.AuditTrailExportService/ListAuditTrailExports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailExportService/ListAuditTrailExports"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "audit_trail", "billing_address", "region"));
        var endpoint = new AuditTrailNotificationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamAuditTrailNoticesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamAuditTrailNoticesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamAuditTrailNotices(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
