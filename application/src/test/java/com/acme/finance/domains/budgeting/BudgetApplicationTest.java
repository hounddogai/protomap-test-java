package com.acme.finance.domains.budgeting;

import com.acme.audit.v1.PullRetentionPoliciesRequest;
import com.acme.audit.v1.PullRetentionPoliciesResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BatchGetBudgetsRequest;
import com.acme.budgeting.v1.BatchGetBudgetsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineCardSettlement;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.CheckForecastEligibilityRequest;
import com.acme.budgeting.v1.CheckForecastEligibilityResponse;
import com.acme.budgeting.v1.CountBudgetsRequest;
import com.acme.budgeting.v1.CountBudgetsResponse;
import com.acme.budgeting.v1.CreateBudgetRequest;
import com.acme.budgeting.v1.CreateBudgetResponse;
import com.acme.budgeting.v1.DeleteBudgetRequest;
import com.acme.budgeting.v1.DeleteBudgetResponse;
import com.acme.budgeting.v1.ExplainBudgetCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetCalculationResponse;
import com.acme.budgeting.v1.ExplainVarianceDecisionRequest;
import com.acme.budgeting.v1.ExplainVarianceDecisionResponse;
import com.acme.budgeting.v1.ExportBudgetLinesRequest;
import com.acme.budgeting.v1.ExportBudgetLinesResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCardSettlement;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.GetBudgetRequest;
import com.acme.budgeting.v1.GetBudgetResponse;
import com.acme.budgeting.v1.LintBudgetsRequest;
import com.acme.budgeting.v1.LintBudgetsResponse;
import com.acme.budgeting.v1.ListBudgetsRequest;
import com.acme.budgeting.v1.ListBudgetsResponse;
import com.acme.budgeting.v1.NormalizeForecastRequest;
import com.acme.budgeting.v1.NormalizeForecastResponse;
import com.acme.budgeting.v1.PreviewBudgetLineRequest;
import com.acme.budgeting.v1.PreviewBudgetLineResponse;
import com.acme.budgeting.v1.PreviewForecastRequest;
import com.acme.budgeting.v1.PreviewForecastResponse;
import com.acme.budgeting.v1.ResolveBudgetsRequest;
import com.acme.budgeting.v1.ResolveBudgetsResponse;
import com.acme.budgeting.v1.ScoreForecastRequest;
import com.acme.budgeting.v1.ScoreForecastResponse;
import com.acme.budgeting.v1.SimulateBudgetLineRequest;
import com.acme.budgeting.v1.SimulateBudgetLineResponse;
import com.acme.budgeting.v1.SimulateBudgetRequest;
import com.acme.budgeting.v1.SimulateBudgetResponse;
import com.acme.budgeting.v1.UpdateBudgetRequest;
import com.acme.budgeting.v1.UpdateBudgetResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.budgeting.v1.WatchBudgetsRequest;
import com.acme.budgeting.v1.WatchBudgetsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.corporateevents.v1.CloneCorporateEventRequest;
import com.acme.corporateevents.v1.CloneCorporateEventResponse;
import com.acme.corporateevents.v1.CorporateEvent;
import com.acme.corporateevents.v1.CorporateEventParticipant;
import com.acme.corporateevents.v1.CorporateEventServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.ExplainUserDecisionRequest;
import com.acme.identity.v1.ExplainUserDecisionResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.NormalizeStressTestRequest;
import com.acme.risk.v1.NormalizeStressTestResponse;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class BudgetApplicationTest {
    @Test
    void watchBudgetsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new BudgetApplication(
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                BudgetCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineQueryServiceGrpc.newBlockingStub(channel),
                BudgetLineValidationServiceGrpc.newBlockingStub(channel),
                BudgetQueryServiceGrpc.newBlockingStub(channel),
                BudgetValidationServiceGrpc.newBlockingStub(channel),
                ForecastValidationServiceGrpc.newBlockingStub(channel),
                ForecastValidationServiceGrpc.newFutureStub(channel),
                VarianceValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CorporateEventServiceGrpc.newBlockingStub(channel),
                UserValidationServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                new BudgetRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioQueryServiceGrpc.newBlockingStub(channel),
                StressTestValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(WatchBudgetsRequest.getDefaultInstance());
        var response = application.watchBudgets(request);
        var sent0 = channel.request("acme.budgeting.v1.ForecastValidationService/PreviewForecast");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(sent0, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        var sent1 = channel.request("acme.risk.v1.RiskScenarioQueryService/CountRiskScenarios");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "query"),
                ProbeSupport.value(sent1, "filter", "query"));
        var sent2 = channel.request("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastValidationService/PreviewForecast"), "forecast", "card", "last4"),
                ProbeSupport.value(sent2, "budget_line", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastValidationService/PreviewForecast"), "forecast", "billing_address", "lines"),
                ProbeSupport.value(sent2, "details", "destination", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastValidationService/PreviewForecast"), "forecast", "card", "token"),
                ProbeSupport.value(sent2, "budget_line", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastValidationService/PreviewForecast"), "forecast", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "details", "destination", "country_code"));
        var sent3 = channel.request("acme.budgeting.v1.BudgetValidationService/LintBudgets");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine"), "budget_line", "audit", "updated_by"),
                ProbeSupport.value(sent3, "budget", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastValidationService/PreviewForecast"), "forecast", "counterparty", "display_name"),
                ProbeSupport.value(sent3, "budget", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine"), "budget_line", "audit", "created_at"),
                ProbeSupport.value(sent3, "budget", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine"), "budget_line", "card", "network"),
                ProbeSupport.value(sent3, "budget", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastValidationService/PreviewForecast"), "forecast", "audit", "updated_at"),
                ProbeSupport.value(response, "budget", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioQueryService/CountRiskScenarios"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioQueryService/CountRiskScenarios"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine"), "budget_line", "number"),
                ProbeSupport.value(response, "budget", "holder", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetValidationService/LintBudgets"), "budget", "budget_id"),
                ProbeSupport.value(response, "budget", "budget_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(response, "budget", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(response, "budget", "contact", "name"));
        var endpoint = new BudgetEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<WatchBudgetsResponse> observer = new StreamObserver<>() {
            public void onNext(WatchBudgetsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.watchBudgets(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void batchCreateBudgetsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new BudgetApplication(
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                BudgetCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineQueryServiceGrpc.newBlockingStub(channel),
                BudgetLineValidationServiceGrpc.newBlockingStub(channel),
                BudgetQueryServiceGrpc.newBlockingStub(channel),
                BudgetValidationServiceGrpc.newBlockingStub(channel),
                ForecastValidationServiceGrpc.newBlockingStub(channel),
                ForecastValidationServiceGrpc.newFutureStub(channel),
                VarianceValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CorporateEventServiceGrpc.newBlockingStub(channel),
                UserValidationServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                new BudgetRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioQueryServiceGrpc.newBlockingStub(channel),
                StressTestValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(BatchCreateBudgetsRequest.getDefaultInstance());
        var response = application.batchCreateBudgets(request);
        var sent0 = channel.request("acme.budgeting.v1.VarianceValidationService/ExplainVarianceDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "header", "submitted_by", "name"),
                ProbeSupport.value(sent0, "variance", "requester", "name"));
        var sent1 = channel.request("acme.risk.v1.StressTestValidationService/NormalizeStressTest");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceValidationService/ExplainVarianceDecision"), "variance", "audit", "updated_at"),
                ProbeSupport.value(sent1, "stress_test", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceValidationService/ExplainVarianceDecision"), "variance", "audit", "created_at"),
                ProbeSupport.value(sent1, "stress_test", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "created_by"),
                ProbeSupport.value(sent1, "stress_test", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceValidationService/ExplainVarianceDecision"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestValidationService/NormalizeStressTest"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        var endpoint = new BudgetEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreateBudgetsResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreateBudgetsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreateBudgets(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
