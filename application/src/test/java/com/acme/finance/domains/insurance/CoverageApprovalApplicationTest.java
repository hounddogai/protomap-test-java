package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.CheckAccrualEligibilityRequest;
import com.acme.accounting.v1.CheckAccrualEligibilityResponse;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.ListAuditEventVersionsRequest;
import com.acme.audit.v1.ListAuditEventVersionsResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastBankSettlement;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ExplainRenewalDecisionRequest;
import com.acme.contracts.v1.ExplainRenewalDecisionResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.customers.v1.CalculateCustomerSegmentRequest;
import com.acme.customers.v1.CalculateCustomerSegmentResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentCondition;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.customers.v1.ResolveHouseholdsRequest;
import com.acme.customers.v1.ResolveHouseholdsResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveCoverageRequest;
import com.acme.insurance.v1.ApproveCoverageResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.CommentPolicyApprovalRequest;
import com.acme.insurance.v1.CommentPolicyApprovalResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.DelegateCoverageApprovalRequest;
import com.acme.insurance.v1.DelegateCoverageApprovalResponse;
import com.acme.insurance.v1.EscalateCoverageRequest;
import com.acme.insurance.v1.EscalateCoverageResponse;
import com.acme.insurance.v1.ListPendingCoveragesRequest;
import com.acme.insurance.v1.ListPendingCoveragesResponse;
import com.acme.insurance.v1.PausePolicyWorkflowRequest;
import com.acme.insurance.v1.PausePolicyWorkflowResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.RecallCoverageRequest;
import com.acme.insurance.v1.RecallCoverageResponse;
import com.acme.insurance.v1.RecallPolicyRequest;
import com.acme.insurance.v1.RecallPolicyResponse;
import com.acme.insurance.v1.RejectCoverageRequest;
import com.acme.insurance.v1.RejectCoverageResponse;
import com.acme.insurance.v1.SubmitCoverageRequest;
import com.acme.insurance.v1.SubmitCoverageResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.risk.v1.CheckStressTestEligibilityRequest;
import com.acme.risk.v1.CheckStressTestEligibilityResponse;
import com.acme.risk.v1.LintStressTestsRequest;
import com.acme.risk.v1.LintStressTestsResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCardSettlement;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestLineTax;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionResponse;
import com.acme.suppliers.v1.NormalizeSupplierRatingRequest;
import com.acme.suppliers.v1.NormalizeSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
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

class CoverageApprovalApplicationTest {
    @Test
    void recallCoveragePreservesPlannedFields() {
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
        var listener = new CoverageApprovalApplicationEvents(AccrualValidationServiceGrpc.newStub(channel), StressTestValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof CoverageApprovalApplication.ApproveCoverageStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof CoverageApprovalApplication.ListPendingCoveragesStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new CoverageApprovalApplication(
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                ForecastCalculationServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdQueryServiceGrpc.newFutureStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                events,
                PolicyApprovalServiceGrpc.newBlockingStub(channel),
                PolicyWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                StressTestValidationServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RecallCoverageRequest.getDefaultInstance());
        var response = application.recallCoverage(request);
        var sent0 = channel.request("acme.insurance.v1.PolicyApprovalService/RecallPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "display_name"),
                ProbeSupport.value(sent0, "details", "approver", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent0, "details", "approver", "user_id"));
        var sent1 = channel.request("acme.suppliers.v1.SupplierRatingValidationService/NormalizeSupplierRating");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyApprovalService/RecallPolicy"), "policy", "card", "network"),
                ProbeSupport.value(sent1, "supplier_rating", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyApprovalService/RecallPolicy"), "policy", "contact", "email"),
                ProbeSupport.value(sent1, "supplier_rating", "counterparty", "email"));
        var sent2 = channel.request("acme.documents.v1.DocumentValidationService/PreviewDocument");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/NormalizeSupplierRating"), "supplier_rating", "audit", "updated_by"),
                ProbeSupport.value(sent2, "document", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyApprovalService/RecallPolicy"), "policy", "contact", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyApprovalService/RecallPolicy"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/NormalizeSupplierRating"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/NormalizeSupplierRating"), "supplier_rating", "audit", "updated_by"),
                ProbeSupport.value(response, "coverage", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage_id"),
                ProbeSupport.value(response, "coverage", "coverage_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "coverage", "parameters"),
                ProbeSupport.value(response, "coverage", "parameters"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new CoverageApprovalApplicationEvents(AccrualValidationServiceGrpc.newStub(channel), StressTestValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CheckStressTestEligibilityResponse>();
        listener.on(new CoverageApprovalApplication.ApproveCoverageStage0Requested(ProbeSupport.sample(CheckStressTestEligibilityRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<CheckAccrualEligibilityResponse>();
        listener.on(new CoverageApprovalApplication.ListPendingCoveragesStage0Requested(ProbeSupport.sample(CheckAccrualEligibilityRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
