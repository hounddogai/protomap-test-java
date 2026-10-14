package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.CancelAuditTrailExportRequest;
import com.acme.audit.v1.CancelAuditTrailExportResponse;
import com.acme.audit.v1.GetAuditTrailExportRequest;
import com.acme.audit.v1.GetAuditTrailExportResponse;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetryAuditTrailExportRequest;
import com.acme.audit.v1.RetryAuditTrailExportResponse;
import com.acme.audit.v1.ScheduleAuditTrailExportRequest;
import com.acme.audit.v1.ScheduleAuditTrailExportResponse;
import com.acme.audit.v1.ShareAuditEventExportRequest;
import com.acme.audit.v1.ShareAuditEventExportResponse;
import com.acme.audit.v1.ShareAuditTrailExportRequest;
import com.acme.audit.v1.ShareAuditTrailExportResponse;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.BrowseAccessPoliciesRequest;
import com.acme.authorization.v1.BrowseAccessPoliciesResponse;
import com.acme.authorization.v1.ExportAccessPoliciesRequest;
import com.acme.authorization.v1.ExportAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class AuditTrailExportApplicationTest {
    @Test
    void streamAuditTrailExportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new AuditTrailExportApplication(
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(StreamAuditTrailExportRequest.getDefaultInstance());
        var response = application.streamAuditTrailExport(request);
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "audit_trail", "billing_address", "region"));
        var endpoint = new AuditTrailExportEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamAuditTrailExportResponse> observer = new StreamObserver<>() {
            public void onNext(StreamAuditTrailExportResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamAuditTrailExport(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void shareAuditTrailExportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new AuditTrailExportApplication(
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ShareAuditTrailExportRequest.getDefaultInstance());
        var response = application.shareAuditTrailExport(request);
        var sent0 = channel.request("acme.authorization.v1.AccessPolicyQueryService/ExportAccessPolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "variables"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail_id"),
                ProbeSupport.value(response, "audit_trail", "audit_trail_id"));
    }
}
