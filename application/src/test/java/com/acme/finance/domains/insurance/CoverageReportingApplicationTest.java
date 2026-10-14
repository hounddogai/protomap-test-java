package com.acme.finance.domains.insurance;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.NormalizePreferenceRequest;
import com.acme.customers.v1.NormalizePreferenceResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.expenses.v1.BatchCreateExpenseReportsRequest;
import com.acme.expenses.v1.BatchCreateExpenseReportsResponse;
import com.acme.expenses.v1.ExpenseReportServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.CountCoverageTotalsRequest;
import com.acme.insurance.v1.CountCoverageTotalsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.DeleteCoverageReportRequest;
import com.acme.insurance.v1.DeleteCoverageReportResponse;
import com.acme.insurance.v1.FetchCoverageReportRequest;
import com.acme.insurance.v1.FetchCoverageReportResponse;
import com.acme.insurance.v1.GenerateCoverageReportRequest;
import com.acme.insurance.v1.GenerateCoverageReportResponse;
import com.acme.insurance.v1.LintPoliciesRequest;
import com.acme.insurance.v1.LintPoliciesResponse;
import com.acme.insurance.v1.ListCoverageReportsRequest;
import com.acme.insurance.v1.ListCoverageReportsResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCardSettlement;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.ScheduleCoverageReportRequest;
import com.acme.insurance.v1.ScheduleCoverageReportResponse;
import com.acme.insurance.v1.ShareCoverageReportRequest;
import com.acme.insurance.v1.ShareCoverageReportResponse;
import com.acme.insurance.v1.StreamCoverageMetricsRequest;
import com.acme.insurance.v1.StreamCoverageMetricsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.insurance.v1.ValidatePremiumRequest;
import com.acme.insurance.v1.ValidatePremiumResponse;
import com.acme.insurance.v1.ValidateUnderwritingImportRequest;
import com.acme.insurance.v1.ValidateUnderwritingImportResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchAdminServiceGrpc;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.RepairPostingBatchRequest;
import com.acme.ledger.v1.RepairPostingBatchResponse;
import com.acme.ledger.v1.RetryFiscalPeriodExportRequest;
import com.acme.ledger.v1.RetryFiscalPeriodExportResponse;
import com.acme.ledger.v1.SendPostingBatchTestNoticeRequest;
import com.acme.ledger.v1.SendPostingBatchTestNoticeResponse;
import com.acme.ledger.v1.StreamJournalEntryNoticesRequest;
import com.acme.ledger.v1.StreamJournalEntryNoticesResponse;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.ledger.v1.SuggestJournalEntriesRequest;
import com.acme.ledger.v1.SuggestJournalEntriesResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ResolveLoansRequest;
import com.acme.lending.v1.ResolveLoansResponse;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.acme.lending.v1.SimulateRepaymentRequest;
import com.acme.lending.v1.SimulateRepaymentResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class CoverageReportingApplicationTest {
    @Test
    void generateCoverageReportPreservesPlannedFields() {
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
        var listener = new CoverageReportingApplicationEvents(PolicyValidationServiceGrpc.newStub(channel), PremiumValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof CoverageReportingApplication.ShareCoverageReportStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof CoverageReportingApplication.DeleteCoverageReportStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new CoverageReportingApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                events,
                ExpenseReportServiceGrpc.newStub(channel),
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                UnderwritingImportServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                PostingBatchAdminServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                CollateralCalculationServiceGrpc.newBlockingStub(channel),
                LoanQueryServiceGrpc.newFutureStub(channel),
                new CoverageReportingRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(GenerateCoverageReportRequest.getDefaultInstance());
        var response = application.generateCoverageReport(request);
        var sent0 = channel.request("acme.insurance.v1.UnderwritingImportService/ValidateUnderwritingImport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "underwriting", "assignee", "user_id"));
        var sent1 = channel.request("acme.ledger.v1.PostingBatchNotificationService/SendPostingBatchTestNotice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingImportService/ValidateUnderwritingImport"), "underwriting", "requester", "name"),
                ProbeSupport.value(sent1, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingImportService/ValidateUnderwritingImport"), "underwriting", "assignee", "email"),
                ProbeSupport.value(sent1, "details", "recipient", "email"));
        var sent2 = channel.request("acme.lending.v1.RepaymentCalculationService/SimulateRepayment");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchNotificationService/SendPostingBatchTestNotice"), "posting_batch", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "repayment", "organizer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingImportService/ValidateUnderwritingImport"), "underwriting", "assignee", "email"),
                ProbeSupport.value(sent2, "repayment", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchNotificationService/SendPostingBatchTestNotice"), "posting_batch", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "details", "destination", "country_code"));
        var sent3 = channel.request("acme.ledger.v1.JournalEntryNotificationService/StreamJournalEntryNotices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingImportService/ValidateUnderwritingImport"), "underwriting", "assignee", "email"),
                ProbeSupport.value(sent3, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingImportService/ValidateUnderwritingImport"), "underwriting", "requester", "preferred_channel"),
                ProbeSupport.value(sent3, "subscriber", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingImportService/ValidateUnderwritingImport"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentCalculationService/SimulateRepayment"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentCalculationService/SimulateRepayment"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }

    @Test
    void streamCoverageMetricsPreservesPlannedFields() {
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
        var listener = new CoverageReportingApplicationEvents(PolicyValidationServiceGrpc.newStub(channel), PremiumValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof CoverageReportingApplication.ShareCoverageReportStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof CoverageReportingApplication.DeleteCoverageReportStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new CoverageReportingApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                events,
                ExpenseReportServiceGrpc.newStub(channel),
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                UnderwritingImportServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                PostingBatchAdminServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                CollateralCalculationServiceGrpc.newBlockingStub(channel),
                LoanQueryServiceGrpc.newFutureStub(channel),
                new CoverageReportingRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(StreamCoverageMetricsRequest.getDefaultInstance());
        var response = application.streamCoverageMetrics(request);
        var sent0 = channel.request("acme.lending.v1.CollateralCalculationService/SimulateCollateral");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "collateral", "owner", "email"));
        var sent1 = channel.request("acme.insurance.v1.CoverageCalculationService/CalculateCoverage");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/SimulateCollateral"), "collateral", "address", "lines"),
                ProbeSupport.value(sent1, "details", "destination", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/SimulateCollateral"), "collateral", "address", "locality"),
                ProbeSupport.value(sent1, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/SimulateCollateral"), "collateral", "audit", "etag"),
                ProbeSupport.value(sent1, "coverage", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/SimulateCollateral"), "collateral", "owner", "user_id"),
                ProbeSupport.value(sent1, "coverage", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/SimulateCollateral"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/SimulateCollateral"), "collateral", "audit", "created_by"),
                ProbeSupport.value(response, "coverage", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageCalculationService/CalculateCoverage"), "coverage", "owner"),
                ProbeSupport.value(response, "coverage", "owner"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageCalculationService/CalculateCoverage"), "coverage", "scopes"),
                ProbeSupport.value(response, "coverage", "scopes"));
        var endpoint = new CoverageReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamCoverageMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamCoverageMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamCoverageMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new CoverageReportingApplicationEvents(PolicyValidationServiceGrpc.newStub(channel), PremiumValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<LintPoliciesResponse>();
        listener.on(new CoverageReportingApplication.ShareCoverageReportStage1Requested(ProbeSupport.sample(LintPoliciesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<ValidatePremiumResponse>();
        listener.on(new CoverageReportingApplication.DeleteCoverageReportStage1Requested(ProbeSupport.sample(ValidatePremiumRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
