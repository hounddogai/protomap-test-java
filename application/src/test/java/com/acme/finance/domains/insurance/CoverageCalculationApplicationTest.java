package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.ResolveAuditEventsRequest;
import com.acme.audit.v1.ResolveAuditEventsResponse;
import com.acme.audit.v1.RevertAuditEventRequest;
import com.acme.audit.v1.RevertAuditEventResponse;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.CompareCoverageOptionsRequest;
import com.acme.insurance.v1.CompareCoverageOptionsResponse;
import com.acme.insurance.v1.ComparePolicyOptionsRequest;
import com.acme.insurance.v1.ComparePolicyOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.EstimateCoverageRequest;
import com.acme.insurance.v1.EstimateCoverageResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.insurance.v1.RecalculateCoveragesRequest;
import com.acme.insurance.v1.RecalculateCoveragesResponse;
import com.acme.insurance.v1.RecalculatePoliciesRequest;
import com.acme.insurance.v1.RecalculatePoliciesResponse;
import com.acme.insurance.v1.SimulateCoverageRequest;
import com.acme.insurance.v1.SimulateCoverageResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersRequest;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersResponse;
import com.acme.localization.v1.ResolveTranslationKeysRequest;
import com.acme.localization.v1.ResolveTranslationKeysResponse;
import com.acme.localization.v1.SummarizeTranslationKeysRequest;
import com.acme.localization.v1.SummarizeTranslationKeysResponse;
import com.acme.localization.v1.TranslationKeyQueryServiceGrpc;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class CoverageCalculationApplicationTest {
    @Test
    void explainCoverageCalculationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InsuranceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveClaim(String tenant, Claim value) {}
            public void saveCoverage(String tenant, Coverage value) {}
            public void savePolicy(String tenant, Policy value) {}
            public void savePremium(String tenant, Premium value) {}
            public void saveUnderwriting(String tenant, Underwriting value) {}
        };
        var listener = new CoverageCalculationApplicationEvents(AuditTrailExportServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof CoverageCalculationApplication.CalculateCoverageStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new CoverageCalculationApplication(
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                ClaimCalculationServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                TranslationKeyQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExplainCoverageCalculationRequest.getDefaultInstance());
        var response = application.explainCoverageCalculation(request);
        var sent0 = channel.request("acme.audit.v1.AuditEventHistoryService/RevertAuditEvent");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "owner", "display_name"),
                ProbeSupport.value(sent0, "audit_event", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        var sent1 = channel.request("acme.insurance.v1.ClaimCalculationService/RecalculateClaims");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "audit", "created_at"),
                ProbeSupport.value(sent1, "claim", "comments", "created_at"));
        var sent2 = channel.request("acme.audit.v1.AuditEventNotificationService/SubscribeAuditEventUpdates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/RecalculateClaims"), "claim", "requester", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "recipient", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "email"),
                ProbeSupport.value(sent2, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/RecalculateClaims"), "claim", "requester", "phone"),
                ProbeSupport.value(sent2, "details", "recipient", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/RecalculateClaims"), "claim", "requester", "name"),
                ProbeSupport.value(sent2, "details", "recipient", "name"));
        var sent3 = channel.request("acme.localization.v1.TranslationKeyQueryService/SummarizeTranslationKeys");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "block", "reason"),
                ProbeSupport.value(sent3, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/RevertAuditEvent"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/RecalculateClaims"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/RecalculateClaims"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventNotificationService/SubscribeAuditEventUpdates"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.localization.v1.TranslationKeyQueryService/SummarizeTranslationKeys"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage_id"),
                ProbeSupport.value(response, "coverage", "coverage_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "description"),
                ProbeSupport.value(response, "coverage", "description"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new CoverageCalculationApplicationEvents(AuditTrailExportServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ListAuditTrailExportsResponse>();
        listener.on(new CoverageCalculationApplication.CalculateCoverageStage1Requested(ProbeSupport.sample(ListAuditTrailExportsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
