package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.CancelAuditEventExportRequest;
import com.acme.audit.v1.CancelAuditEventExportResponse;
import com.acme.audit.v1.GetAuditEventExportRequest;
import com.acme.audit.v1.GetAuditEventExportResponse;
import com.acme.audit.v1.ListAuditEventExportsRequest;
import com.acme.audit.v1.ListAuditEventExportsResponse;
import com.acme.audit.v1.PullAuditTrailsRequest;
import com.acme.audit.v1.PullAuditTrailsResponse;
import com.acme.audit.v1.ReconcileAuditTrailsRequest;
import com.acme.audit.v1.ReconcileAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetryAuditEventExportRequest;
import com.acme.audit.v1.RetryAuditEventExportResponse;
import com.acme.audit.v1.ScheduleAuditEventExportRequest;
import com.acme.audit.v1.ScheduleAuditEventExportResponse;
import com.acme.audit.v1.ShareAuditEventExportRequest;
import com.acme.audit.v1.ShareAuditEventExportResponse;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SendPostingBatchTestNoticeRequest;
import com.acme.ledger.v1.SendPostingBatchTestNoticeResponse;
import com.google.protobuf.Any;
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

class AuditEventExportApplicationTest {
    @Test
    void streamAuditEventExportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new AuditEventExportApplication(
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PostingBatchNotificationServiceGrpc.newFutureStub(channel),
                new AuditEventExportRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(StreamAuditEventExportRequest.getDefaultInstance());
        var response = application.streamAuditEventExport(request);
        var sent0 = channel.request("acme.audit.v1.AuditTrailSyncService/ReconcileAuditTrails");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "audit_trail", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent0, "audit_trail", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "audit_trail", "metadata"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "deadline"),
                ProbeSupport.value(sent0, "audit_trail", "issued_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/ReconcileAuditTrails"), "audit_trail", "counterparty", "display_name"),
                ProbeSupport.value(response, "audit_event", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/ReconcileAuditTrails"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        var endpoint = new AuditEventExportEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamAuditEventExportResponse> observer = new StreamObserver<>() {
            public void onNext(StreamAuditEventExportResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamAuditEventExport(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
