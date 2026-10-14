package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.CountBudgetLineTotalsRequest;
import com.acme.budgeting.v1.CountBudgetLineTotalsResponse;
import com.acme.budgeting.v1.DeleteBudgetLineReportRequest;
import com.acme.budgeting.v1.DeleteBudgetLineReportResponse;
import com.acme.budgeting.v1.ExplainForecastDecisionRequest;
import com.acme.budgeting.v1.ExplainForecastDecisionResponse;
import com.acme.budgeting.v1.ExplainVarianceDecisionRequest;
import com.acme.budgeting.v1.ExplainVarianceDecisionResponse;
import com.acme.budgeting.v1.ExportBudgetLinesRequest;
import com.acme.budgeting.v1.ExportBudgetLinesResponse;
import com.acme.budgeting.v1.FetchBudgetLineReportRequest;
import com.acme.budgeting.v1.FetchBudgetLineReportResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastCardSettlement;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.GenerateBudgetLineReportRequest;
import com.acme.budgeting.v1.GenerateBudgetLineReportResponse;
import com.acme.budgeting.v1.ListBudgetLineReportsRequest;
import com.acme.budgeting.v1.ListBudgetLineReportsResponse;
import com.acme.budgeting.v1.ListVarianceImportsRequest;
import com.acme.budgeting.v1.ListVarianceImportsResponse;
import com.acme.budgeting.v1.PushForecastRequest;
import com.acme.budgeting.v1.PushForecastResponse;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.budgeting.v1.ScheduleBudgetLineReportRequest;
import com.acme.budgeting.v1.ScheduleBudgetLineReportResponse;
import com.acme.budgeting.v1.ScoreVarianceRequest;
import com.acme.budgeting.v1.ScoreVarianceResponse;
import com.acme.budgeting.v1.ShareBudgetLineReportRequest;
import com.acme.budgeting.v1.ShareBudgetLineReportResponse;
import com.acme.budgeting.v1.StreamBudgetLineMetricsRequest;
import com.acme.budgeting.v1.StreamBudgetLineMetricsResponse;
import com.acme.budgeting.v1.ValidateForecastRequest;
import com.acme.budgeting.v1.ValidateForecastResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.BrowseUsersRequest;
import com.acme.identity.v1.BrowseUsersResponse;
import com.acme.identity.v1.CountUsersRequest;
import com.acme.identity.v1.CountUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.AdvancePolicyWorkflowRequest;
import com.acme.insurance.v1.AdvancePolicyWorkflowResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCardSettlement;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.SearchLedgerAccountsRequest;
import com.acme.ledger.v1.SearchLedgerAccountsResponse;
import com.acme.risk.v1.AdvanceExposureWorkflowRequest;
import com.acme.risk.v1.AdvanceExposureWorkflowResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.CancelRiskScenarioWorkflowRequest;
import com.acme.risk.v1.CancelRiskScenarioWorkflowResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioStatusChange;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SummarizeSearchIndexesRequest;
import com.acme.search.v1.SummarizeSearchIndexesResponse;
import com.acme.suppliers.v1.PreviewSupplierRatingRequest;
import com.acme.suppliers.v1.PreviewSupplierRatingResponse;
import com.acme.suppliers.v1.ScoreSupplierRatingRequest;
import com.acme.suppliers.v1.ScoreSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
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

class BudgetLineReportingApplicationTest {
    @Test
    void streamBudgetLineMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var listener = new BudgetLineReportingApplicationEvents(UserQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof BudgetLineReportingApplication.ListBudgetLineReportsStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new BudgetLineReportingApplication(
                DisposalSyncServiceGrpc.newStub(channel),
                BudgetLineQueryServiceGrpc.newBlockingStub(channel),
                ForecastCalculationServiceGrpc.newBlockingStub(channel),
                ForecastSyncServiceGrpc.newBlockingStub(channel),
                ForecastValidationServiceGrpc.newBlockingStub(channel),
                VarianceImportServiceGrpc.newBlockingStub(channel),
                VarianceValidationServiceGrpc.newBlockingStub(channel),
                events,
                UserQueryServiceGrpc.newBlockingStub(channel),
                PolicyWorkflowServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newBlockingStub(channel),
                repository,
                ExposureWorkflowServiceGrpc.newBlockingStub(channel),
                RiskRegisterServiceGrpc.newStub(channel),
                RiskScenarioWorkflowServiceGrpc.newBlockingStub(channel),
                SearchIndexQueryServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamBudgetLineMetricsRequest.getDefaultInstance());
        var response = application.streamBudgetLineMetrics(request);
        var sent0 = channel.request("acme.budgeting.v1.BudgetLineQueryService/ExportBudgetLines");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "statuses"),
                ProbeSupport.value(sent0, "filter", "statuses"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.search.v1.SearchIndexQueryService/SummarizeSearchIndexes");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent1, "details", "filters"));
        var sent2 = channel.request("acme.risk.v1.ExposureWorkflowService/AdvanceExposureWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent2, "details", "notify", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(sent2, "details", "notify", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent2, "exposure", "contact", "email"));
        var sent3 = channel.request("acme.insurance.v1.PremiumSyncService/ResolvePremiumConflict");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/AdvanceExposureWorkflow"), "exposure", "audit", "created_by"),
                ProbeSupport.value(sent3, "premium", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/AdvanceExposureWorkflow"), "exposure", "card", "holder_name"),
                ProbeSupport.value(sent3, "premium", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/AdvanceExposureWorkflow"), "exposure", "audit", "updated_by"),
                ProbeSupport.value(sent3, "premium", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/AdvanceExposureWorkflow"), "exposure", "contact", "email"),
                ProbeSupport.value(sent3, "premium", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.search.v1.SearchIndexQueryService/SummarizeSearchIndexes"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/AdvanceExposureWorkflow"), "exposure", "contact", "email"),
                ProbeSupport.value(response, "budget_line", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumSyncService/ResolvePremiumConflict"), "premium", "billing_address", "locality"),
                ProbeSupport.value(response, "budget_line", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumSyncService/ResolvePremiumConflict"), "premium", "billing_address", "country_code"),
                ProbeSupport.value(response, "budget_line", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "budget_line", "billing_address", "region"));
        var endpoint = new BudgetLineReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamBudgetLineMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamBudgetLineMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamBudgetLineMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new BudgetLineReportingApplicationEvents(UserQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<BrowseUsersResponse>();
        listener.on(new BudgetLineReportingApplication.ListBudgetLineReportsStage0Requested(ProbeSupport.sample(BrowseUsersRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
