package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventAdminServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.CheckAuditEventEligibilityRequest;
import com.acme.audit.v1.CheckAuditEventEligibilityResponse;
import com.acme.audit.v1.ExplainAuditEventDecisionRequest;
import com.acme.audit.v1.ExplainAuditEventDecisionResponse;
import com.acme.audit.v1.GetAuditEventRulesRequest;
import com.acme.audit.v1.GetAuditEventRulesResponse;
import com.acme.audit.v1.LintAuditEventsRequest;
import com.acme.audit.v1.LintAuditEventsResponse;
import com.acme.audit.v1.LockAuditEventRequest;
import com.acme.audit.v1.LockAuditEventResponse;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.PreviewAuditEventRequest;
import com.acme.audit.v1.PreviewAuditEventResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.audit.v1.SyncRetentionPoliciesRequest;
import com.acme.audit.v1.SyncRetentionPoliciesResponse;
import com.acme.audit.v1.ValidateAuditEventRequest;
import com.acme.audit.v1.ValidateAuditEventResponse;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class AuditEventValidationApplicationTest {
    @Test
    void scoreAuditEventPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var listener = new AuditEventValidationApplicationEvents(RetentionPolicySyncServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof AuditEventValidationApplication.GetAuditEventRulesStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new AuditEventValidationApplication(
                AuditEventAdminServiceGrpc.newBlockingStub(channel),
                events,
                LedgerAccountQueryServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ScoreAuditEventRequest.getDefaultInstance());
        var response = application.scoreAuditEvent(request);
        var sent0 = channel.request("acme.ledger.v1.LedgerAccountQueryService/BrowseLedgerAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_event", "occurred_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_event", "subject", "audit_trail_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_event", "attributes"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountQueryService/BrowseLedgerAccounts"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountQueryService/BrowseLedgerAccounts"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_event_id"),
                ProbeSupport.value(response, "audit_event", "audit_event_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_event", "kind"),
                ProbeSupport.value(response, "audit_event", "kind"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new AuditEventValidationApplicationEvents(RetentionPolicySyncServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<SyncRetentionPoliciesResponse>();
        listener.on(new AuditEventValidationApplication.GetAuditEventRulesStage0Requested(ProbeSupport.sample(SyncRetentionPoliciesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
