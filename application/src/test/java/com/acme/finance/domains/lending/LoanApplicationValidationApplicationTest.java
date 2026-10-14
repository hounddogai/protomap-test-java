package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.PullAuditTrailsRequest;
import com.acme.audit.v1.PullAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.ListDocumentsRequest;
import com.acme.documents.v1.ListDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.FindUsersRequest;
import com.acme.identity.v1.FindUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.PremiumCardSettlement;
import com.acme.insurance.v1.RecalculatePremiumsRequest;
import com.acme.insurance.v1.RecalculatePremiumsResponse;
import com.acme.lending.v1.CalculateLoanRequest;
import com.acme.lending.v1.CalculateLoanResponse;
import com.acme.lending.v1.CheckLoanApplicationEligibilityRequest;
import com.acme.lending.v1.CheckLoanApplicationEligibilityResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CompareLoanOptionsRequest;
import com.acme.lending.v1.CompareLoanOptionsResponse;
import com.acme.lending.v1.EstimateLoanApplicationRequest;
import com.acme.lending.v1.EstimateLoanApplicationResponse;
import com.acme.lending.v1.ExplainLoanApplicationCalculationRequest;
import com.acme.lending.v1.ExplainLoanApplicationCalculationResponse;
import com.acme.lending.v1.ExplainLoanApplicationDecisionRequest;
import com.acme.lending.v1.ExplainLoanApplicationDecisionResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.GetLoanApplicationRulesRequest;
import com.acme.lending.v1.GetLoanApplicationRulesResponse;
import com.acme.lending.v1.LintLoanApplicationsRequest;
import com.acme.lending.v1.LintLoanApplicationsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.lending.v1.NormalizeLoanApplicationRequest;
import com.acme.lending.v1.NormalizeLoanApplicationResponse;
import com.acme.lending.v1.PreviewLoanApplicationRequest;
import com.acme.lending.v1.PreviewLoanApplicationResponse;
import com.acme.lending.v1.RecalculateLoansRequest;
import com.acme.lending.v1.RecalculateLoansResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.lending.v1.ValidateLoanApplicationRequest;
import com.acme.lending.v1.ValidateLoanApplicationResponse;
import com.acme.search.v1.CountSearchIndexesRequest;
import com.acme.search.v1.CountSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
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

class LoanApplicationValidationApplicationTest {
    @Test
    void previewLoanApplicationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var listener = new LoanApplicationValidationApplicationEvents(RetentionPolicyValidationServiceGrpc.newStub(channel), LoanCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof LoanApplicationValidationApplication.PreviewLoanApplicationStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof LoanApplicationValidationApplication.PreviewLoanApplicationStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new LoanApplicationValidationApplication(
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                events,
                UserQueryServiceGrpc.newBlockingStub(channel),
                PremiumCalculationServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                LoanCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(PreviewLoanApplicationRequest.getDefaultInstance());
        var response = application.previewLoanApplication(request);
        var sent0 = channel.request("acme.lending.v1.LoanCalculationService/RecalculateLoans");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "audit", "created_by"),
                ProbeSupport.value(sent0, "loan", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "card", "token"),
                ProbeSupport.value(sent0, "loan", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "billing_address", "lines"),
                ProbeSupport.value(sent0, "details", "destination", "lines"));
        var sent1 = channel.request("acme.audit.v1.RetentionPolicyValidationService/ScoreRetentionPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "audit", "created_at"),
                ProbeSupport.value(sent1, "retention_policy", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "billing_address", "region"),
                ProbeSupport.value(sent1, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanCalculationService/RecalculateLoans"), "loan", "audit", "etag"),
                ProbeSupport.value(sent1, "retention_policy", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanCalculationService/RecalculateLoans"), "loan", "contact", "email"),
                ProbeSupport.value(sent1, "retention_policy", "owner", "email"));
        var sent2 = channel.request("acme.lending.v1.LoanCalculationService/SimulateLoan");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyValidationService/ScoreRetentionPolicy"), "retention_policy", "audit", "updated_at"),
                ProbeSupport.value(sent2, "loan", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanCalculationService/RecalculateLoans"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyValidationService/ScoreRetentionPolicy"), "retention_policy", "audit", "etag"),
                ProbeSupport.value(response, "loan_application", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyValidationService/ScoreRetentionPolicy"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application_id"),
                ProbeSupport.value(response, "loan_application", "loan_application_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_application", "billing_address"),
                ProbeSupport.value(response, "loan_application", "billing_address"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new LoanApplicationValidationApplicationEvents(RetentionPolicyValidationServiceGrpc.newStub(channel), LoanCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ScoreRetentionPolicyResponse>();
        listener.on(new LoanApplicationValidationApplication.PreviewLoanApplicationStage1Requested(ProbeSupport.sample(ScoreRetentionPolicyRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<SimulateLoanResponse>();
        listener.on(new LoanApplicationValidationApplication.PreviewLoanApplicationStage2Requested(ProbeSupport.sample(SimulateLoanRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
