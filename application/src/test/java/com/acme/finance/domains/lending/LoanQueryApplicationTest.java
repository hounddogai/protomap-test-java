package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.RevertAuditTrailRequest;
import com.acme.audit.v1.RevertAuditTrailResponse;
import com.acme.audit.v1.StreamAuditTrailNoticesRequest;
import com.acme.audit.v1.StreamAuditTrailNoticesResponse;
import com.acme.banking.v1.PreviewStandingOrderRequest;
import com.acme.banking.v1.PreviewStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.LintFeatureFlagsRequest;
import com.acme.featureflags.v1.LintFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.ValidatePolicyRequest;
import com.acme.insurance.v1.ValidatePolicyResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.RetryFiscalPeriodExportRequest;
import com.acme.ledger.v1.RetryFiscalPeriodExportResponse;
import com.acme.lending.v1.BrowseLoanApplicationsRequest;
import com.acme.lending.v1.BrowseLoanApplicationsResponse;
import com.acme.lending.v1.BrowseLoansRequest;
import com.acme.lending.v1.BrowseLoansResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CountLoansRequest;
import com.acme.lending.v1.CountLoansResponse;
import com.acme.lending.v1.ExportLoansRequest;
import com.acme.lending.v1.ExportLoansResponse;
import com.acme.lending.v1.FindLoansRequest;
import com.acme.lending.v1.FindLoansResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LookupLoanRequest;
import com.acme.lending.v1.LookupLoanResponse;
import com.acme.lending.v1.NormalizeLoanApplicationRequest;
import com.acme.lending.v1.NormalizeLoanApplicationResponse;
import com.acme.lending.v1.PeekLoanRequest;
import com.acme.lending.v1.PeekLoanResponse;
import com.acme.lending.v1.QuoteLoanRequest;
import com.acme.lending.v1.QuoteLoanResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.ResolveLoansRequest;
import com.acme.lending.v1.ResolveLoansResponse;
import com.acme.lending.v1.SimulateLoanApplicationRequest;
import com.acme.lending.v1.SimulateLoanApplicationResponse;
import com.acme.lending.v1.SuggestLoansRequest;
import com.acme.lending.v1.SuggestLoansResponse;
import com.acme.lending.v1.SummarizeLoansRequest;
import com.acme.lending.v1.SummarizeLoansResponse;
import com.acme.lending.v1.ValidateLoanApplicationRequest;
import com.acme.lending.v1.ValidateLoanApplicationResponse;
import com.acme.localization.v1.SearchTranslationKeysRequest;
import com.acme.localization.v1.SearchTranslationKeysResponse;
import com.acme.localization.v1.TranslationKeyServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class LoanQueryApplicationTest {
    @Test
    void exportLoansPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var listener = new LoanQueryApplicationEvents(CoverageCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof LoanQueryApplication.BrowseLoansStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new LoanQueryApplication(
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                AuditTrailNotificationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                FeatureFlagServiceGrpc.newFutureStub(channel),
                FeatureFlagValidationServiceGrpc.newFutureStub(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                LoanApplicationQueryServiceGrpc.newBlockingStub(channel),
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                LoanCalculationServiceGrpc.newBlockingStub(channel),
                TranslationKeyServiceGrpc.newBlockingStub(channel),
                new LoanQueryRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(ExportLoansRequest.getDefaultInstance());
        var response = application.exportLoans(request);
        var sent0 = channel.request("acme.localization.v1.TranslationKeyService/SearchTranslationKeys");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        var sent1 = channel.request("acme.audit.v1.AuditTrailNotificationService/StreamAuditTrailNotices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.localization.v1.TranslationKeyService/SearchTranslationKeys"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(response, "loan", "labels"));
        var endpoint = new LoanQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportLoansResponse> observer = new StreamObserver<>() {
            public void onNext(ExportLoansResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportLoans(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeLoansPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var listener = new LoanQueryApplicationEvents(CoverageCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof LoanQueryApplication.BrowseLoansStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new LoanQueryApplication(
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                AuditTrailNotificationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                FeatureFlagServiceGrpc.newFutureStub(channel),
                FeatureFlagValidationServiceGrpc.newFutureStub(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                LoanApplicationQueryServiceGrpc.newBlockingStub(channel),
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                LoanCalculationServiceGrpc.newBlockingStub(channel),
                TranslationKeyServiceGrpc.newBlockingStub(channel),
                new LoanQueryRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(SummarizeLoansRequest.getDefaultInstance());
        var response = application.summarizeLoans(request);
        var sent0 = channel.request("acme.audit.v1.AuditTrailHistoryService/RevertAuditTrail");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "author_email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "filters"),
                ProbeSupport.value(sent0, "audit_trail", "lines", "attributes"));
        var sent1 = channel.request("acme.banking.v1.StandingOrderValidationService/PreviewStandingOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/RevertAuditTrail"), "audit_trail", "billing_address", "locality"),
                ProbeSupport.value(sent1, "standing_order", "location", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/RevertAuditTrail"), "audit_trail", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "standing_order", "organizer", "display_name"));
        var sent2 = channel.request("acme.lending.v1.LoanApplicationValidationService/ValidateLoanApplication");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/PreviewStandingOrder"), "standing_order", "organizer", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/RevertAuditTrail"), "audit_trail", "audit", "etag"),
                ProbeSupport.value(sent2, "loan_application", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/RevertAuditTrail"), "audit_trail", "card", "last4"),
                ProbeSupport.value(sent2, "loan_application", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/RevertAuditTrail"), "audit_trail", "card", "holder_name"),
                ProbeSupport.value(sent2, "loan_application", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/RevertAuditTrail"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/RevertAuditTrail"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationValidationService/ValidateLoanApplication"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationValidationService/ValidateLoanApplication"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new LoanQueryApplicationEvents(CoverageCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CalculateCoverageResponse>();
        listener.on(new LoanQueryApplication.BrowseLoansStage0Requested(ProbeSupport.sample(CalculateCoverageRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
