package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.EstimateReconciliationRequest;
import com.acme.accounting.v1.EstimateReconciliationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.LintAuditEventsRequest;
import com.acme.audit.v1.LintAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.ScoreObligationRequest;
import com.acme.contracts.v1.ScoreObligationResponse;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.PreviewCustomerSegmentRequest;
import com.acme.customers.v1.PreviewCustomerSegmentResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ListPremiumConflictsRequest;
import com.acme.insurance.v1.ListPremiumConflictsResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PreviewCoverageRequest;
import com.acme.insurance.v1.PreviewCoverageResponse;
import com.acme.insurance.v1.PreviewPolicyRequest;
import com.acme.insurance.v1.PreviewPolicyResponse;
import com.acme.insurance.v1.PreviewUnderwritingRequest;
import com.acme.insurance.v1.PreviewUnderwritingResponse;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.PushPremiumRequest;
import com.acme.insurance.v1.PushPremiumResponse;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.insurance.v1.StreamPremiumChangesRequest;
import com.acme.insurance.v1.StreamPremiumChangesResponse;
import com.acme.insurance.v1.SummarizePoliciesRequest;
import com.acme.insurance.v1.SummarizePoliciesResponse;
import com.acme.insurance.v1.SyncPremiumsRequest;
import com.acme.insurance.v1.SyncPremiumsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.payroll.v1.CheckPayStubEligibilityRequest;
import com.acme.payroll.v1.CheckPayStubEligibilityResponse;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCardSettlement;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class PremiumSyncApplicationTest {
    @Test
    void syncPremiumsPreservesPlannedFields() {
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
        var application = new PremiumSyncApplication(
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                UnderwritingValidationServiceGrpc.newBlockingStub(channel),
                PayStubValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(SyncPremiumsRequest.getDefaultInstance());
        var response = application.syncPremiums(request);
        var sent0 = channel.request("acme.audit.v1.AuditEventValidationService/LintAuditEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "lines", "attributes"),
                ProbeSupport.value(sent0, "audit_event", "attributes"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "counterparty", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "counterparty", "display_name"),
                ProbeSupport.value(sent0, "audit_event", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "lines", "line_id"),
                ProbeSupport.value(sent0, "audit_event", "source"));
        var sent1 = channel.request("acme.accounting.v1.ReconciliationCalculationService/EstimateReconciliation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "billing_address", "lines"),
                ProbeSupport.value(sent1, "details", "destination", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "counterparty", "email"),
                ProbeSupport.value(sent1, "reconciliation", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "reconciliation", "customer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "audit", "updated_at"),
                ProbeSupport.value(sent1, "reconciliation", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationCalculationService/EstimateReconciliation"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium_id"),
                ProbeSupport.value(response, "premium", "premium_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "premium", "number"),
                ProbeSupport.value(response, "premium", "number"));
    }

    @Test
    void streamPremiumChangesPreservesPlannedFields() {
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
        var application = new PremiumSyncApplication(
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                UnderwritingValidationServiceGrpc.newBlockingStub(channel),
                PayStubValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(StreamPremiumChangesRequest.getDefaultInstance());
        var response = application.streamPremiumChanges(request);
        var sent0 = channel.request("acme.insurance.v1.PolicyValidationService/PreviewPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "bank", "bic"),
                ProbeSupport.value(sent0, "policy", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "counterparty", "email"),
                ProbeSupport.value(sent0, "policy", "contact", "email"));
        var sent1 = channel.request("acme.insurance.v1.UnderwritingValidationService/PreviewUnderwriting");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "audit", "created_at"),
                ProbeSupport.value(sent1, "underwriting", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyValidationService/PreviewPolicy"), "policy", "audit", "etag"),
                ProbeSupport.value(response, "premium", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingValidationService/PreviewUnderwriting"), "underwriting", "audit", "updated_by"),
                ProbeSupport.value(response, "premium", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingValidationService/PreviewUnderwriting"), "underwriting", "assignee", "email"),
                ProbeSupport.value(response, "premium", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "issued_at"),
                ProbeSupport.value(response, "premium", "issued_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "ack", "sequence"),
                ProbeSupport.value(response, "sequence"));
        var endpoint = new PremiumSyncEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamPremiumChangesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamPremiumChangesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.streamPremiumChanges(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "value", "completed"), signals);
    }
}
