package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.GetAuditTrailSyncStateRequest;
import com.acme.audit.v1.GetAuditTrailSyncStateResponse;
import com.acme.audit.v1.ListAuditTrailConflictsRequest;
import com.acme.audit.v1.ListAuditTrailConflictsResponse;
import com.acme.audit.v1.PullAuditTrailsRequest;
import com.acme.audit.v1.PullAuditTrailsResponse;
import com.acme.audit.v1.PushAuditTrailRequest;
import com.acme.audit.v1.PushAuditTrailResponse;
import com.acme.audit.v1.ReconcileAuditTrailsRequest;
import com.acme.audit.v1.ReconcileAuditTrailsResponse;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.SyncAuditTrailsRequest;
import com.acme.audit.v1.SyncAuditTrailsResponse;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesResponse;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class AuditTrailSyncApplicationTest {
    @Test
    void resolveAuditTrailConflictPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var listener = new AuditTrailSyncApplicationEvents(JournalEntryValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof AuditTrailSyncApplication.GetAuditTrailSyncStateStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new AuditTrailSyncApplication(
                events,
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ResolveAuditTrailConflictRequest.getDefaultInstance());
        var response = application.resolveAuditTrailConflict(request);
        var sent0 = channel.request("acme.ledger.v1.JournalEntryNotificationService/SubscribeJournalEntryUpdates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryNotificationService/SubscribeJournalEntryUpdates"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryNotificationService/SubscribeJournalEntryUpdates"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail_id"),
                ProbeSupport.value(response, "audit_trail", "audit_trail_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail", "due_at"),
                ProbeSupport.value(response, "audit_trail", "due_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new AuditTrailSyncApplicationEvents(JournalEntryValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<NormalizeJournalEntryResponse>();
        listener.on(new AuditTrailSyncApplication.GetAuditTrailSyncStateStage0Requested(ProbeSupport.sample(NormalizeJournalEntryRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
