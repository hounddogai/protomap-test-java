package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RevertAuditTrailRequest;
import com.acme.audit.v1.RevertAuditTrailResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.BrowseBudgetsRequest;
import com.acme.budgeting.v1.BrowseBudgetsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLimit;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineCardSettlement;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.CalculateVarianceRequest;
import com.acme.budgeting.v1.CalculateVarianceResponse;
import com.acme.budgeting.v1.CheckBudgetLineEligibilityRequest;
import com.acme.budgeting.v1.CheckBudgetLineEligibilityResponse;
import com.acme.budgeting.v1.CompareVarianceOptionsRequest;
import com.acme.budgeting.v1.CompareVarianceOptionsResponse;
import com.acme.budgeting.v1.CountBudgetsRequest;
import com.acme.budgeting.v1.CountBudgetsResponse;
import com.acme.budgeting.v1.ExplainForecastDecisionRequest;
import com.acme.budgeting.v1.ExplainForecastDecisionResponse;
import com.acme.budgeting.v1.ExportBudgetsRequest;
import com.acme.budgeting.v1.ExportBudgetsResponse;
import com.acme.budgeting.v1.FindBudgetsRequest;
import com.acme.budgeting.v1.FindBudgetsResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.GetBudgetRulesRequest;
import com.acme.budgeting.v1.GetBudgetRulesResponse;
import com.acme.budgeting.v1.LookupBudgetRequest;
import com.acme.budgeting.v1.LookupBudgetResponse;
import com.acme.budgeting.v1.PeekBudgetRequest;
import com.acme.budgeting.v1.PeekBudgetResponse;
import com.acme.budgeting.v1.QuoteBudgetLineRequest;
import com.acme.budgeting.v1.QuoteBudgetLineResponse;
import com.acme.budgeting.v1.ResolveBudgetsRequest;
import com.acme.budgeting.v1.ResolveBudgetsResponse;
import com.acme.budgeting.v1.SuggestBudgetsRequest;
import com.acme.budgeting.v1.SuggestBudgetsResponse;
import com.acme.budgeting.v1.SummarizeBudgetsRequest;
import com.acme.budgeting.v1.SummarizeBudgetsResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceCalculationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.PreviewClaimRequest;
import com.acme.insurance.v1.PreviewClaimResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeRequest;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeResponse;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesResponse;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.SimulateLoanApplicationRequest;
import com.acme.lending.v1.SimulateLoanApplicationResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
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

class BudgetQueryApplicationTest {
    @Test
    void exportBudgetsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var listener = new BudgetQueryApplicationEvents(ControlCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof BudgetQueryApplication.PeekBudgetStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new BudgetQueryApplication(
                FixedAssetServiceGrpc.newStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineValidationServiceGrpc.newBlockingStub(channel),
                ForecastValidationServiceGrpc.newBlockingStub(channel),
                VarianceCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                ClaimValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                new BudgetQueryRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(ExportBudgetsRequest.getDefaultInstance());
        var response = application.exportBudgets(request);
        var sent0 = channel.request("acme.lending.v1.LoanApplicationCalculationService/SimulateLoanApplication");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent0, "loan_application", "audit", "updated_at"));
        var sent1 = channel.request("acme.ledger.v1.FiscalPeriodNotificationService/SendFiscalPeriodTestNotice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationCalculationService/SimulateLoanApplication"), "loan_application", "counterparty", "email"),
                ProbeSupport.value(sent1, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationCalculationService/SimulateLoanApplication"), "result", "details"),
                ProbeSupport.value(sent1, "details", "variables"));
        var sent2 = channel.request("acme.budgeting.v1.BudgetLineCalculationService/QuoteBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationCalculationService/SimulateLoanApplication"), "loan_application", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "budget_line", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationCalculationService/SimulateLoanApplication"), "loan_application", "billing_address", "recipient"),
                ProbeSupport.value(sent2, "budget_line", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationCalculationService/SimulateLoanApplication"), "loan_application", "card", "holder_name"),
                ProbeSupport.value(sent2, "budget_line", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationCalculationService/SimulateLoanApplication"), "loan_application", "audit", "created_at"),
                ProbeSupport.value(response, "budget", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/QuoteBudgetLine"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(response, "budget", "labels"));
        var endpoint = new BudgetQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportBudgetsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportBudgetsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportBudgets(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeBudgetsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var listener = new BudgetQueryApplicationEvents(ControlCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof BudgetQueryApplication.PeekBudgetStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new BudgetQueryApplication(
                FixedAssetServiceGrpc.newStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineValidationServiceGrpc.newBlockingStub(channel),
                ForecastValidationServiceGrpc.newBlockingStub(channel),
                VarianceCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                ClaimValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                new BudgetQueryRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(SummarizeBudgetsRequest.getDefaultInstance());
        var response = application.summarizeBudgets(request);
        var sent0 = channel.request("acme.ledger.v1.FiscalPeriodNotificationService/SubscribeFiscalPeriodUpdates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        var sent1 = channel.request("acme.insurance.v1.ClaimValidationService/PreviewClaim");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodNotificationService/SubscribeFiscalPeriodUpdates"), "fiscal_period", "audit", "created_by"),
                ProbeSupport.value(sent1, "claim", "audit", "created_by"));
        var sent2 = channel.request("acme.budgeting.v1.ForecastValidationService/ExplainForecastDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodNotificationService/SubscribeFiscalPeriodUpdates"), "fiscal_period", "audit", "created_at"),
                ProbeSupport.value(sent2, "forecast", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/PreviewClaim"), "claim", "audit", "created_by"),
                ProbeSupport.value(sent2, "forecast", "audit", "created_by"));
        var sent3 = channel.request("acme.budgeting.v1.BudgetValidationService/GetBudgetRules");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_id"),
                ProbeSupport.value(sent3, "budget_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodNotificationService/SubscribeFiscalPeriodUpdates"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/PreviewClaim"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastValidationService/ExplainForecastDecision"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetValidationService/GetBudgetRules"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new BudgetQueryApplicationEvents(ControlCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<EstimateControlResponse>();
        listener.on(new BudgetQueryApplication.PeekBudgetStage0Requested(ProbeSupport.sample(EstimateControlRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
