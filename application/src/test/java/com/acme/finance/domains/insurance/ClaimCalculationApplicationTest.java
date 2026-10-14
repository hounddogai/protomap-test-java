package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RevertRetentionPolicyRequest;
import com.acme.audit.v1.RevertRetentionPolicyResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.BrowseAccessPoliciesRequest;
import com.acme.authorization.v1.BrowseAccessPoliciesResponse;
import com.acme.authorization.v1.SummarizeAccessPoliciesRequest;
import com.acme.authorization.v1.SummarizeAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateClaimRequest;
import com.acme.insurance.v1.CalculateClaimResponse;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.CompareClaimOptionsRequest;
import com.acme.insurance.v1.CompareClaimOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.EstimateClaimRequest;
import com.acme.insurance.v1.EstimateClaimResponse;
import com.acme.insurance.v1.EstimatePolicyRequest;
import com.acme.insurance.v1.EstimatePolicyResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.GetClaimRatesRequest;
import com.acme.insurance.v1.GetClaimRatesResponse;
import com.acme.insurance.v1.GetPolicyRatesRequest;
import com.acme.insurance.v1.GetPolicyRatesResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.QuoteClaimRequest;
import com.acme.insurance.v1.QuoteClaimResponse;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.insurance.v1.SimulateClaimRequest;
import com.acme.insurance.v1.SimulateClaimResponse;
import com.acme.insurance.v1.SimulatePolicyRequest;
import com.acme.insurance.v1.SimulatePolicyResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ValidatePostingBatchRequest;
import com.acme.ledger.v1.ValidatePostingBatchResponse;
import com.acme.onboarding.v1.OnboardingPlanServiceGrpc;
import com.acme.onboarding.v1.SearchOnboardingPlansRequest;
import com.acme.onboarding.v1.SearchOnboardingPlansResponse;
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
import org.springframework.stereotype.Service;

class ClaimCalculationApplicationTest {
    @Test
    void estimateClaimPreservesPlannedFields() {
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
        var application = new ClaimCalculationApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                new ClaimCalculationRequestMapperImpl(),
                OnboardingPlanServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(EstimateClaimRequest.getDefaultInstance());
        var response = application.estimateClaim(request);
        var sent0 = channel.request("acme.authorization.v1.AccessPolicyQueryService/SummarizeAccessPolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        var sent1 = channel.request("acme.insurance.v1.PolicyCalculationService/ExplainPolicyCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "requester", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "requester", "phone"),
                ProbeSupport.value(sent1, "policy", "contact", "phone"));
        var sent2 = channel.request("acme.insurance.v1.PolicyCalculationService/EstimatePolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/ExplainPolicyCalculation"), "policy", "holder", "version"),
                ProbeSupport.value(sent2, "policy", "holder", "version"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "audit", "etag"),
                ProbeSupport.value(sent2, "policy", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyQueryService/SummarizeAccessPolicies"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyQueryService/SummarizeAccessPolicies"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/ExplainPolicyCalculation"), "policy", "contact", "email"),
                ProbeSupport.value(response, "claim", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/ExplainPolicyCalculation"), "policy", "currency_code"),
                ProbeSupport.value(response, "breakdown", "currency_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/EstimatePolicy"), "policy", "contact", "email"),
                ProbeSupport.value(response, "claim", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim_id"),
                ProbeSupport.value(response, "claim", "claim_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "claim", "priority"),
                ProbeSupport.value(response, "claim", "priority"));
    }
}
