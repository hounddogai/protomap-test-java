package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventMeasurement;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.ExplainAuditEventDecisionRequest;
import com.acme.audit.v1.ExplainAuditEventDecisionResponse;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.dispatch.v1.DispatchOrderServiceGrpc;
import com.acme.dispatch.v1.WatchDispatchOrdersRequest;
import com.acme.dispatch.v1.WatchDispatchOrdersResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CheckPolicyEligibilityRequest;
import com.acme.insurance.v1.CheckPolicyEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimChannelOrigin;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.EstimateClaimRequest;
import com.acme.insurance.v1.EstimateClaimResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.ExplainPolicyDecisionRequest;
import com.acme.insurance.v1.ExplainPolicyDecisionResponse;
import com.acme.insurance.v1.GetPolicyRulesRequest;
import com.acme.insurance.v1.GetPolicyRulesResponse;
import com.acme.insurance.v1.LintPoliciesRequest;
import com.acme.insurance.v1.LintPoliciesResponse;
import com.acme.insurance.v1.NormalizePolicyRequest;
import com.acme.insurance.v1.NormalizePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PreviewPolicyRequest;
import com.acme.insurance.v1.PreviewPolicyResponse;
import com.acme.insurance.v1.QuoteClaimRequest;
import com.acme.insurance.v1.QuoteClaimResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.insurance.v1.RecalculatePoliciesRequest;
import com.acme.insurance.v1.RecalculatePoliciesResponse;
import com.acme.insurance.v1.SimulateClaimRequest;
import com.acme.insurance.v1.SimulateClaimResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.ValidatePolicyRequest;
import com.acme.insurance.v1.ValidatePolicyResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesResponse;
import com.acme.performance.v1.FindReviewsRequest;
import com.acme.performance.v1.FindReviewsResponse;
import com.acme.performance.v1.ReviewQueryServiceGrpc;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class PolicyValidationApplicationTest {
    @Test
    void validatePolicyPreservesPlannedFields() {
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
        var listener = new PolicyValidationApplicationEvents(PolicyCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PolicyValidationApplication.CheckPolicyEligibilityStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PolicyValidationApplication(
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                DispatchOrderServiceGrpc.newBlockingStub(channel),
                events,
                ClaimCalculationServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                new PolicyValidationRequestMapperImpl(),
                ReviewQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ValidatePolicyRequest.getDefaultInstance());
        var response = application.validatePolicy(request);
        var sent0 = channel.request("acme.insurance.v1.ClaimCalculationService/EstimateClaim");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy", "audit", "updated_by"),
                ProbeSupport.value(sent0, "claim", "audit", "updated_by"));
        var sent1 = channel.request("acme.insurance.v1.ClaimCalculationService/SimulateClaim");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/EstimateClaim"), "claim", "channel", "address"),
                ProbeSupport.value(sent1, "claim", "channel", "address"));
        var sent2 = channel.request("acme.insurance.v1.ClaimCalculationService/ExplainClaimCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "email"),
                ProbeSupport.value(sent2, "claim", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/SimulateClaim"), "claim", "customer", "email"),
                ProbeSupport.value(sent2, "claim", "customer", "email"));
        var sent3 = channel.request("acme.insurance.v1.PolicyCalculationService/RecalculatePolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/SimulateClaim"), "claim", "audit", "created_at"),
                ProbeSupport.value(sent3, "policy", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/EstimateClaim"), "claim", "customer", "email"),
                ProbeSupport.value(response, "policy", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/EstimateClaim"), "claim", "requester", "phone"),
                ProbeSupport.value(response, "policy", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/SimulateClaim"), "breakdown", "currency_code"),
                ProbeSupport.value(response, "policy", "currency_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/ExplainClaimCalculation"), "claim", "requester", "preferred_channel"),
                ProbeSupport.value(response, "policy", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/RecalculatePolicies"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/RecalculatePolicies"), "policy", "status"),
                ProbeSupport.value(response, "policy", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy_id"),
                ProbeSupport.value(response, "policy", "policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy", "balance"),
                ProbeSupport.value(response, "policy", "balance"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new PolicyValidationApplicationEvents(PolicyCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<RecalculatePoliciesResponse>();
        listener.on(new PolicyValidationApplication.CheckPolicyEligibilityStage2Requested(ProbeSupport.sample(RecalculatePoliciesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
