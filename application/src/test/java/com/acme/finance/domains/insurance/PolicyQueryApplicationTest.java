package com.acme.finance.domains.insurance;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.PushAuditTrailRequest;
import com.acme.audit.v1.PushAuditTrailResponse;
import com.acme.audit.v1.RevertAuditTrailRequest;
import com.acme.audit.v1.RevertAuditTrailResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.BrowsePoliciesRequest;
import com.acme.insurance.v1.BrowsePoliciesResponse;
import com.acme.insurance.v1.CheckCoverageEligibilityRequest;
import com.acme.insurance.v1.CheckCoverageEligibilityResponse;
import com.acme.insurance.v1.CheckPremiumEligibilityRequest;
import com.acme.insurance.v1.CheckPremiumEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.ComparePolicyOptionsRequest;
import com.acme.insurance.v1.ComparePolicyOptionsResponse;
import com.acme.insurance.v1.CountPoliciesRequest;
import com.acme.insurance.v1.CountPoliciesResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ExportPoliciesRequest;
import com.acme.insurance.v1.ExportPoliciesResponse;
import com.acme.insurance.v1.FindPoliciesRequest;
import com.acme.insurance.v1.FindPoliciesResponse;
import com.acme.insurance.v1.LookupPolicyRequest;
import com.acme.insurance.v1.LookupPolicyResponse;
import com.acme.insurance.v1.NormalizePolicyRequest;
import com.acme.insurance.v1.NormalizePolicyResponse;
import com.acme.insurance.v1.PeekPolicyRequest;
import com.acme.insurance.v1.PeekPolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.RecalculatePremiumsRequest;
import com.acme.insurance.v1.RecalculatePremiumsResponse;
import com.acme.insurance.v1.ResolvePoliciesRequest;
import com.acme.insurance.v1.ResolvePoliciesResponse;
import com.acme.insurance.v1.ScoreCoverageRequest;
import com.acme.insurance.v1.ScoreCoverageResponse;
import com.acme.insurance.v1.SimulateUnderwritingRequest;
import com.acme.insurance.v1.SimulateUnderwritingResponse;
import com.acme.insurance.v1.SuggestPoliciesRequest;
import com.acme.insurance.v1.SuggestPoliciesResponse;
import com.acme.insurance.v1.SummarizePoliciesRequest;
import com.acme.insurance.v1.SummarizePoliciesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.insurance.v1.ValidateClaimRequest;
import com.acme.insurance.v1.ValidateClaimResponse;
import com.acme.insurance.v1.ValidateCoverageRequest;
import com.acme.insurance.v1.ValidateCoverageResponse;
import com.acme.ledger.v1.BrowseJournalEntriesRequest;
import com.acme.ledger.v1.BrowseJournalEntriesResponse;
import com.acme.suppliers.v1.SupplierRef;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class PolicyQueryApplicationTest {
    @Test
    void exportPoliciesPreservesPlannedFields() {
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
        var listener = new PolicyQueryApplicationEvents(AuditTrailHistoryServiceGrpc.newStub(channel), PremiumValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PolicyQueryApplication.SummarizePoliciesStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof PolicyQueryApplication.SummarizePoliciesStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PolicyQueryApplication(
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                ClaimValidationServiceGrpc.newBlockingStub(channel),
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                PremiumCalculationServiceGrpc.newBlockingStub(channel),
                UnderwritingCalculationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ExportPoliciesRequest.getDefaultInstance());
        var response = application.exportPolicies(request);
        var sent0 = channel.request("acme.insurance.v1.UnderwritingCalculationService/SimulateUnderwriting");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "details", "destination", "region"));
        var sent1 = channel.request("acme.insurance.v1.PremiumCalculationService/RecalculatePremiums");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/SimulateUnderwriting"), "underwriting", "audit", "created_at"),
                ProbeSupport.value(sent1, "premium", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/SimulateUnderwriting"), "underwriting", "audit", "updated_by"),
                ProbeSupport.value(sent1, "premium", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/SimulateUnderwriting"), "underwriting", "sla", "due_at"),
                ProbeSupport.value(sent1, "premium", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/SimulateUnderwriting"), "underwriting", "assignee", "display_name"),
                ProbeSupport.value(sent1, "premium", "counterparty", "display_name"));
        var sent2 = channel.request("acme.insurance.v1.CoverageValidationService/ValidateCoverage");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/SimulateUnderwriting"), "underwriting", "assignee", "email"),
                ProbeSupport.value(sent2, "coverage", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumCalculationService/RecalculatePremiums"), "premium", "counterparty", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/SimulateUnderwriting"), "underwriting", "requester", "name"),
                ProbeSupport.value(sent2, "details", "contact", "name"));
        var sent3 = channel.request("acme.insurance.v1.PolicyCalculationService/ComparePolicyOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumCalculationService/RecalculatePremiums"), "premium", "billing_address", "locality"),
                ProbeSupport.value(sent3, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/SimulateUnderwriting"), "underwriting", "audit", "updated_at"),
                ProbeSupport.value(sent3, "policy", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageValidationService/ValidateCoverage"), "coverage", "audit", "etag"),
                ProbeSupport.value(sent3, "policy", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumCalculationService/RecalculatePremiums"), "premium", "card", "token"),
                ProbeSupport.value(sent3, "policy", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/SimulateUnderwriting"), "underwriting", "assignee", "email"),
                ProbeSupport.value(response, "policy", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumCalculationService/RecalculatePremiums"), "premium", "counterparty", "display_name"),
                ProbeSupport.value(response, "policy", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageValidationService/ValidateCoverage"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/ComparePolicyOptions"), "policy", "holder", "coverage_id"),
                ProbeSupport.value(response, "policy", "holder", "coverage_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(response, "policy", "labels"));
        var endpoint = new PolicyQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportPoliciesResponse> observer = new StreamObserver<>() {
            public void onNext(ExportPoliciesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportPolicies(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizePoliciesPreservesPlannedFields() {
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
        var listener = new PolicyQueryApplicationEvents(AuditTrailHistoryServiceGrpc.newStub(channel), PremiumValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PolicyQueryApplication.SummarizePoliciesStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof PolicyQueryApplication.SummarizePoliciesStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PolicyQueryApplication(
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                ClaimValidationServiceGrpc.newBlockingStub(channel),
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                PremiumCalculationServiceGrpc.newBlockingStub(channel),
                UnderwritingCalculationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(SummarizePoliciesRequest.getDefaultInstance());
        var response = application.summarizePolicies(request);
        var sent0 = channel.request("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        var sent1 = channel.request("acme.audit.v1.AuditTrailHistoryService/RevertAuditTrail");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility"), "premium", "audit", "created_at"),
                ProbeSupport.value(sent1, "audit_trail", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility"), "premium", "audit", "updated_at"),
                ProbeSupport.value(sent1, "audit_trail", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility"), "premium", "bank", "iban"),
                ProbeSupport.value(sent1, "audit_trail", "bank", "iban"));
        var sent2 = channel.request("acme.insurance.v1.CoverageValidationService/ScoreCoverage");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility"), "premium", "counterparty", "email"),
                ProbeSupport.value(sent2, "coverage", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility"), "premium", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/RevertAuditTrail"), "audit_trail", "billing_address", "recipient"),
                ProbeSupport.value(sent2, "details", "destination", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/RevertAuditTrail"), "audit_trail", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "coverage", "owner", "display_name"));
        var sent3 = channel.request("acme.audit.v1.AuditTrailSyncService/PushAuditTrail");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility"), "premium", "bank", "bic"),
                ProbeSupport.value(sent3, "audit_trail", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/CheckPremiumEligibility"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageValidationService/ScoreCoverage"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new PolicyQueryApplicationEvents(AuditTrailHistoryServiceGrpc.newStub(channel), PremiumValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CheckPremiumEligibilityResponse>();
        listener.on(new PolicyQueryApplication.SummarizePoliciesStage0Requested(ProbeSupport.sample(CheckPremiumEligibilityRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<RevertAuditTrailResponse>();
        listener.on(new PolicyQueryApplication.SummarizePoliciesStage1Requested(ProbeSupport.sample(RevertAuditTrailRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
