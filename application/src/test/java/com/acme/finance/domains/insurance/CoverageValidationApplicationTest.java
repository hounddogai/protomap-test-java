package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AnnotateAuditTrailVersionRequest;
import com.acme.audit.v1.AnnotateAuditTrailVersionResponse;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.AuditTrailLineTax;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.concessions.v1.ConcessionStand;
import com.acme.concessions.v1.ConcessionStandCalculationServiceGrpc;
import com.acme.concessions.v1.RecalculateConcessionStandsRequest;
import com.acme.concessions.v1.RecalculateConcessionStandsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.CloneUserRequest;
import com.acme.identity.v1.CloneUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.CheckCoverageEligibilityRequest;
import com.acme.insurance.v1.CheckCoverageEligibilityResponse;
import com.acme.insurance.v1.CheckPolicyEligibilityRequest;
import com.acme.insurance.v1.CheckPolicyEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimChannelOrigin;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.CompareClaimOptionsRequest;
import com.acme.insurance.v1.CompareClaimOptionsResponse;
import com.acme.insurance.v1.ComparePolicyOptionsRequest;
import com.acme.insurance.v1.ComparePolicyOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.EstimateCoverageRequest;
import com.acme.insurance.v1.EstimateCoverageResponse;
import com.acme.insurance.v1.ExplainCoverageDecisionRequest;
import com.acme.insurance.v1.ExplainCoverageDecisionResponse;
import com.acme.insurance.v1.GetCoverageRulesRequest;
import com.acme.insurance.v1.GetCoverageRulesResponse;
import com.acme.insurance.v1.NormalizeClaimRequest;
import com.acme.insurance.v1.NormalizeClaimResponse;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.insurance.v1.NormalizePolicyRequest;
import com.acme.insurance.v1.NormalizePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PreviewCoverageRequest;
import com.acme.insurance.v1.PreviewCoverageResponse;
import com.acme.insurance.v1.QuoteClaimRequest;
import com.acme.insurance.v1.QuoteClaimResponse;
import com.acme.insurance.v1.ScoreCoverageRequest;
import com.acme.insurance.v1.ScoreCoverageResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.ValidateCoverageRequest;
import com.acme.insurance.v1.ValidateCoverageResponse;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.RevertJournalEntryRequest;
import com.acme.ledger.v1.RevertJournalEntryResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class CoverageValidationApplicationTest {
    @Test
    void validateCoveragePreservesPlannedFields() {
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
        var application = new CoverageValidationApplication(
                AuditEventServiceGrpc.newStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                ConcessionStandCalculationServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                ClaimCalculationServiceGrpc.newBlockingStub(channel),
                ClaimValidationServiceGrpc.newBlockingStub(channel),
                CoverageCalculationServiceGrpc.newFutureStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newBlockingStub(channel),
                new CoverageValidationRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(ValidateCoverageRequest.getDefaultInstance());
        var response = application.validateCoverage(request);
        var sent0 = channel.request("acme.insurance.v1.ClaimValidationService/NormalizeClaim");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "owner", "email"),
                ProbeSupport.value(sent0, "claim", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "owner", "display_name"),
                ProbeSupport.value(sent0, "claim", "assignee", "display_name"));
        var sent1 = channel.request("acme.insurance.v1.PolicyValidationService/CheckPolicyEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "audit", "created_at"),
                ProbeSupport.value(sent1, "policy", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "phone"),
                ProbeSupport.value(sent1, "policy", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "owner", "email"),
                ProbeSupport.value(sent1, "policy", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/NormalizeClaim"), "claim", "audit", "updated_at"),
                ProbeSupport.value(sent1, "policy", "audit", "updated_at"));
        var sent2 = channel.request("acme.identity.v1.UserService/CloneUser");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "owner", "email"),
                ProbeSupport.value(sent2, "user", "relationship_manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "preferred_channel"),
                ProbeSupport.value(sent2, "user", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/NormalizeClaim"), "claim", "assignee", "display_name"),
                ProbeSupport.value(sent2, "user", "relationship_manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/NormalizeClaim"), "claim", "audit", "created_by"),
                ProbeSupport.value(sent2, "user", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/NormalizeClaim"), "claim", "audit", "updated_by"),
                ProbeSupport.value(response, "coverage", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyValidationService/CheckPolicyEligibility"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/CloneUser"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage_id"),
                ProbeSupport.value(response, "coverage", "coverage_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "parameters"),
                ProbeSupport.value(response, "coverage", "parameters"));
    }
}
