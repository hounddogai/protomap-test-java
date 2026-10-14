package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.CompareAuditEventVersionsRequest;
import com.acme.audit.v1.CompareAuditEventVersionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.audit.v1.ShareAuditTrailExportRequest;
import com.acme.audit.v1.ShareAuditTrailExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.CheckPremiumEligibilityRequest;
import com.acme.insurance.v1.CheckPremiumEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.CompareCoverageOptionsRequest;
import com.acme.insurance.v1.CompareCoverageOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageBlockAction;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.EstimateCoverageRequest;
import com.acme.insurance.v1.EstimateCoverageResponse;
import com.acme.insurance.v1.EstimatePolicyRequest;
import com.acme.insurance.v1.EstimatePolicyResponse;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.GetPremiumRulesRequest;
import com.acme.insurance.v1.GetPremiumRulesResponse;
import com.acme.insurance.v1.LintPoliciesRequest;
import com.acme.insurance.v1.LintPoliciesResponse;
import com.acme.insurance.v1.LintPremiumsRequest;
import com.acme.insurance.v1.LintPremiumsResponse;
import com.acme.insurance.v1.NormalizePremiumRequest;
import com.acme.insurance.v1.NormalizePremiumResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PreviewPremiumRequest;
import com.acme.insurance.v1.PreviewPremiumResponse;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.insurance.v1.ScorePremiumRequest;
import com.acme.insurance.v1.ScorePremiumResponse;
import com.acme.insurance.v1.SimulateCoverageRequest;
import com.acme.insurance.v1.SimulateCoverageResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.ValidatePremiumRequest;
import com.acme.insurance.v1.ValidatePremiumResponse;
import com.acme.performance.v1.CloneReviewRequest;
import com.acme.performance.v1.CloneReviewResponse;
import com.acme.performance.v1.Review;
import com.acme.performance.v1.ReviewCardSettlement;
import com.acme.performance.v1.ReviewServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
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

class PremiumValidationApplicationTest {
    @Test
    void normalizePremiumPreservesPlannedFields() {
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
        var application = new PremiumValidationApplication(
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyValidationServiceGrpc.newBlockingStub(channel),
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                new PremiumValidationRequestMapperImpl(),
                ReviewServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(NormalizePremiumRequest.getDefaultInstance());
        var response = application.normalizePremium(request);
        var sent0 = channel.request("acme.audit.v1.AuditEventHistoryService/CompareAuditEventVersions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "counterparty", "display_name"),
                ProbeSupport.value(sent0, "audit_event", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        var sent1 = channel.request("acme.performance.v1.ReviewService/CloneReview");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "card", "last4"),
                ProbeSupport.value(sent1, "review", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "billing_address", "region"),
                ProbeSupport.value(sent1, "review", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "audit", "updated_by"),
                ProbeSupport.value(sent1, "review", "audit", "updated_by"));
        var sent2 = channel.request("acme.insurance.v1.PolicyCalculationService/QuotePolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.performance.v1.ReviewService/CloneReview"), "review", "card", "last4"),
                ProbeSupport.value(sent2, "policy", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "counterparty", "email"),
                ProbeSupport.value(sent2, "policy", "contact", "email"));
        var sent3 = channel.request("acme.insurance.v1.PolicyValidationService/LintPolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/QuotePolicy"), "policy", "bank", "account_holder"),
                ProbeSupport.value(sent3, "policy", "bank", "account_holder"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/CompareAuditEventVersions"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.performance.v1.ReviewService/CloneReview"), "review", "number"),
                ProbeSupport.value(response, "premium", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/QuotePolicy"), "policy", "audit", "created_at"),
                ProbeSupport.value(response, "premium", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/QuotePolicy"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyValidationService/LintPolicies"), "policy", "audit", "created_by"),
                ProbeSupport.value(response, "premium", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyValidationService/LintPolicies"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium_id"),
                ProbeSupport.value(response, "premium", "premium_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "total"),
                ProbeSupport.value(response, "premium", "total"));
    }
}
