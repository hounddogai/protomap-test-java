package com.acme.finance.domains.insurance;

import com.acme.anomalies.v1.AnomalyQueryServiceGrpc;
import com.acme.anomalies.v1.ResolveAnomaliesRequest;
import com.acme.anomalies.v1.ResolveAnomaliesResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.audit.v1.RevertAuditEventRequest;
import com.acme.audit.v1.RevertAuditEventResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionRequest;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionResponse;
import com.acme.authorization.v1.SummarizeAccessPoliciesRequest;
import com.acme.authorization.v1.SummarizeAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.CountFeatureFlagsRequest;
import com.acme.featureflags.v1.CountFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
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
import com.acme.insurance.v1.CalculatePremiumRequest;
import com.acme.insurance.v1.CalculatePremiumResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.CompareClaimOptionsRequest;
import com.acme.insurance.v1.CompareClaimOptionsResponse;
import com.acme.insurance.v1.ComparePremiumOptionsRequest;
import com.acme.insurance.v1.ComparePremiumOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CoverageRouteAction;
import com.acme.insurance.v1.EstimatePolicyRequest;
import com.acme.insurance.v1.EstimatePolicyResponse;
import com.acme.insurance.v1.EstimatePremiumRequest;
import com.acme.insurance.v1.EstimatePremiumResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.ExplainPremiumCalculationRequest;
import com.acme.insurance.v1.ExplainPremiumCalculationResponse;
import com.acme.insurance.v1.GetPremiumRatesRequest;
import com.acme.insurance.v1.GetPremiumRatesResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.insurance.v1.QuotePremiumRequest;
import com.acme.insurance.v1.QuotePremiumResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.insurance.v1.RecalculatePremiumsRequest;
import com.acme.insurance.v1.RecalculatePremiumsResponse;
import com.acme.insurance.v1.SimulateClaimRequest;
import com.acme.insurance.v1.SimulateClaimResponse;
import com.acme.insurance.v1.SimulateCoverageRequest;
import com.acme.insurance.v1.SimulateCoverageResponse;
import com.acme.insurance.v1.SimulatePremiumRequest;
import com.acme.insurance.v1.SimulatePremiumResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.ValidateClaimRequest;
import com.acme.insurance.v1.ValidateClaimResponse;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsRequest;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsResponse;
import com.acme.recruiting.v1.CalculateJobOpeningRequest;
import com.acme.recruiting.v1.CalculateJobOpeningResponse;
import com.acme.recruiting.v1.InterviewRef;
import com.acme.recruiting.v1.JobOpening;
import com.acme.recruiting.v1.JobOpeningCalculationServiceGrpc;
import com.acme.shows.v1.CalculateShowRequest;
import com.acme.shows.v1.CalculateShowResponse;
import com.acme.shows.v1.QuoteShowRequest;
import com.acme.shows.v1.QuoteShowResponse;
import com.acme.shows.v1.Show;
import com.acme.shows.v1.ShowCalculationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class PremiumCalculationApplicationTest {
    @Test
    void recalculatePremiumsPreservesPlannedFields() {
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
        var listener = new PremiumCalculationApplicationEvents(ClaimCalculationServiceGrpc.newStub(channel), ClaimValidationServiceGrpc.newStub(channel), JobOpeningCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PremiumCalculationApplication.CalculatePremiumStage2Requested command) {
                listener.on(command);
            }
            else if (event instanceof PremiumCalculationApplication.RecalculatePremiumsStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof PremiumCalculationApplication.RecalculatePremiumsStage3Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PremiumCalculationApplication(
                AnomalyQueryServiceGrpc.newBlockingStub(channel),
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                FeatureFlagQueryServiceGrpc.newBlockingStub(channel),
                ClaimCalculationServiceGrpc.newBlockingStub(channel),
                ClaimCalculationServiceGrpc.newFutureStub(channel),
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                new PremiumCalculationRequestMapperImpl(),
                repository,
                Runnable::run,
                ShowCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RecalculatePremiumsRequest.getDefaultInstance());
        var response = application.recalculatePremiums(request);
        var sent0 = channel.request("acme.insurance.v1.ClaimValidationService/ValidateClaim");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "audit", "created_by"),
                ProbeSupport.value(sent0, "claim", "audit", "created_by"));
        var sent1 = channel.request("acme.insurance.v1.ClaimCalculationService/ExplainClaimCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/ValidateClaim"), "claim", "audit", "updated_at"),
                ProbeSupport.value(sent1, "claim", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/ValidateClaim"), "claim", "assignee", "user_id"),
                ProbeSupport.value(sent1, "claim", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/ValidateClaim"), "claim", "sla", "breached"),
                ProbeSupport.value(sent1, "claim", "sla", "breached"));
        var sent2 = channel.request("acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        var sent3 = channel.request("acme.insurance.v1.ClaimCalculationService/RecalculateClaims");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/ValidateClaim"), "claim", "requester", "phone"),
                ProbeSupport.value(sent3, "claim", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/ValidateClaim"), "claim", "assignee", "email"),
                ProbeSupport.value(response, "premium", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/ExplainClaimCalculation"), "claim", "customer", "display_name"),
                ProbeSupport.value(response, "premium", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/ExplainClaimCalculation"), "claim", "audit", "created_at"),
                ProbeSupport.value(response, "premium", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/RecalculateClaims"), "breakdown", "total"),
                ProbeSupport.value(response, "breakdown", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium_id"),
                ProbeSupport.value(response, "premium", "premium_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "issued_at"),
                ProbeSupport.value(response, "premium", "issued_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new PremiumCalculationApplicationEvents(ClaimCalculationServiceGrpc.newStub(channel), ClaimValidationServiceGrpc.newStub(channel), JobOpeningCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CalculateJobOpeningResponse>();
        listener.on(new PremiumCalculationApplication.CalculatePremiumStage2Requested(ProbeSupport.sample(CalculateJobOpeningRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<ValidateClaimResponse>();
        listener.on(new PremiumCalculationApplication.RecalculatePremiumsStage0Requested(ProbeSupport.sample(ValidateClaimRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        var completion2 = new CompletableFuture<RecalculateClaimsResponse>();
        listener.on(new PremiumCalculationApplication.RecalculatePremiumsStage3Requested(ProbeSupport.sample(RecalculateClaimsRequest.getDefaultInstance()), completion2, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion2.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion2.getNow(null));
    }
}
