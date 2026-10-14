package com.acme.finance.domains.budgeting;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.BrowseBudgetLinesRequest;
import com.acme.budgeting.v1.BrowseBudgetLinesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineCardSettlement;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.CalculateBudgetLineRequest;
import com.acme.budgeting.v1.CalculateBudgetLineResponse;
import com.acme.budgeting.v1.CountBudgetLinesRequest;
import com.acme.budgeting.v1.CountBudgetLinesResponse;
import com.acme.budgeting.v1.CountBudgetsRequest;
import com.acme.budgeting.v1.CountBudgetsResponse;
import com.acme.budgeting.v1.EstimateBudgetLineRequest;
import com.acme.budgeting.v1.EstimateBudgetLineResponse;
import com.acme.budgeting.v1.ExplainForecastCalculationRequest;
import com.acme.budgeting.v1.ExplainForecastCalculationResponse;
import com.acme.budgeting.v1.ExportBudgetLinesRequest;
import com.acme.budgeting.v1.ExportBudgetLinesResponse;
import com.acme.budgeting.v1.ExportBudgetsRequest;
import com.acme.budgeting.v1.ExportBudgetsResponse;
import com.acme.budgeting.v1.FindBudgetLinesRequest;
import com.acme.budgeting.v1.FindBudgetLinesResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.LintBudgetsRequest;
import com.acme.budgeting.v1.LintBudgetsResponse;
import com.acme.budgeting.v1.LookupBudgetLineRequest;
import com.acme.budgeting.v1.LookupBudgetLineResponse;
import com.acme.budgeting.v1.PeekBudgetLineRequest;
import com.acme.budgeting.v1.PeekBudgetLineResponse;
import com.acme.budgeting.v1.PreviewBudgetRequest;
import com.acme.budgeting.v1.PreviewBudgetResponse;
import com.acme.budgeting.v1.QuoteBudgetLineRequest;
import com.acme.budgeting.v1.QuoteBudgetLineResponse;
import com.acme.budgeting.v1.RecalculateBudgetLinesRequest;
import com.acme.budgeting.v1.RecalculateBudgetLinesResponse;
import com.acme.budgeting.v1.RecalculateForecastsRequest;
import com.acme.budgeting.v1.RecalculateForecastsResponse;
import com.acme.budgeting.v1.ResolveBudgetLinesRequest;
import com.acme.budgeting.v1.ResolveBudgetLinesResponse;
import com.acme.budgeting.v1.SimulateBudgetLineRequest;
import com.acme.budgeting.v1.SimulateBudgetLineResponse;
import com.acme.budgeting.v1.SimulateVarianceRequest;
import com.acme.budgeting.v1.SimulateVarianceResponse;
import com.acme.budgeting.v1.SuggestBudgetLinesRequest;
import com.acme.budgeting.v1.SuggestBudgetLinesResponse;
import com.acme.budgeting.v1.SummarizeBudgetLinesRequest;
import com.acme.budgeting.v1.SummarizeBudgetLinesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceCalculationServiceGrpc;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchCardSettlement;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ScorePostingBatchRequest;
import com.acme.ledger.v1.ScorePostingBatchResponse;
import com.acme.suppliers.v1.SupplierRef;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class BudgetLineQueryApplicationTest {
    @Test
    void exportBudgetLinesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new BudgetLineQueryApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                BudgetQueryServiceGrpc.newBlockingStub(channel),
                BudgetValidationServiceGrpc.newBlockingStub(channel),
                ForecastCalculationServiceGrpc.newBlockingStub(channel),
                VarianceCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseServiceGrpc.newStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ExportBudgetLinesRequest.getDefaultInstance());
        var response = application.exportBudgetLines(request);
        var sent0 = channel.request("acme.budgeting.v1.VarianceCalculationService/SimulateVariance");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "variance", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceCalculationService/SimulateVariance"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceCalculationService/SimulateVariance"), "variance", "customer", "budget_line_id"),
                ProbeSupport.value(response, "budget_line", "budget_line_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "budget_line", "billing_address", "region"));
        var endpoint = new BudgetLineQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportBudgetLinesResponse> observer = new StreamObserver<>() {
            public void onNext(ExportBudgetLinesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportBudgetLines(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeBudgetLinesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new BudgetLineQueryApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                BudgetQueryServiceGrpc.newBlockingStub(channel),
                BudgetValidationServiceGrpc.newBlockingStub(channel),
                ForecastCalculationServiceGrpc.newBlockingStub(channel),
                VarianceCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseServiceGrpc.newStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(SummarizeBudgetLinesRequest.getDefaultInstance());
        var response = application.summarizeBudgetLines(request);
        var sent0 = channel.request("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line_id"),
                ProbeSupport.value(sent0, "budget_line", "budget_line_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "budget_line", "counterparty", "email"));
        var sent1 = channel.request("acme.budgeting.v1.BudgetValidationService/LintBudgets");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine"), "budget_line", "card", "network"),
                ProbeSupport.value(sent1, "budget", "card", "network"));
        var sent2 = channel.request("acme.budgeting.v1.BudgetLineCalculationService/EstimateBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetValidationService/LintBudgets"), "budget", "card", "network"),
                ProbeSupport.value(sent2, "budget_line", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine"), "budget_line", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine"), "budget_line", "billing_address", "lines"),
                ProbeSupport.value(sent2, "budget_line", "billing_address", "lines"));
        var sent3 = channel.request("acme.budgeting.v1.BudgetLineCalculationService/QuoteBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine"), "budget_line", "card", "network"),
                ProbeSupport.value(sent3, "budget_line", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/SimulateBudgetLine"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetValidationService/LintBudgets"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetValidationService/LintBudgets"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/EstimateBudgetLine"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}
