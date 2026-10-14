package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.BrowseAuditTrailsRequest;
import com.acme.audit.v1.BrowseAuditTrailsResponse;
import com.acme.audit.v1.CountAuditEventsRequest;
import com.acme.audit.v1.CountAuditEventsResponse;
import com.acme.audit.v1.ExportAuditTrailsRequest;
import com.acme.audit.v1.ExportAuditTrailsResponse;
import com.acme.audit.v1.FindAuditTrailsRequest;
import com.acme.audit.v1.FindAuditTrailsResponse;
import com.acme.audit.v1.LookupAuditTrailRequest;
import com.acme.audit.v1.LookupAuditTrailResponse;
import com.acme.audit.v1.ResolveAuditTrailsRequest;
import com.acme.audit.v1.ResolveAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SuggestAuditTrailsRequest;
import com.acme.audit.v1.SuggestAuditTrailsResponse;
import com.acme.audit.v1.SummarizeAuditTrailsRequest;
import com.acme.audit.v1.SummarizeAuditTrailsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesRequest;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class AuditTrailQueryApplicationTest {
    @Test
    void lookupAuditTrailPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new AuditTrailQueryApplication(
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(LookupAuditTrailRequest.getDefaultInstance());
        var response = application.lookupAuditTrail(request);
        var sent0 = channel.request("acme.ledger.v1.FiscalPeriodNotificationService/StreamFiscalPeriodNotices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail_id"),
                ProbeSupport.value(response, "audit_trail", "audit_trail_id"));
    }

    @Test
    void exportAuditTrailsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new AuditTrailQueryApplication(
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ExportAuditTrailsRequest.getDefaultInstance());
        var response = application.exportAuditTrails(request);
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "audit_trail", "billing_address", "region"));
        var endpoint = new AuditTrailQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportAuditTrailsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportAuditTrailsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportAuditTrails(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
