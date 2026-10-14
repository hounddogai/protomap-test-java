package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.ResolveDepreciationsRequest;
import com.acme.assets.v1.ResolveDepreciationsResponse;
import com.acme.assets.v1.WatchFixedAssetsRequest;
import com.acme.assets.v1.WatchFixedAssetsResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.CountBankAccountsRequest;
import com.acme.banking.v1.CountBankAccountsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetBankFunding;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.CalculateBudgetLineRequest;
import com.acme.budgeting.v1.CalculateBudgetLineResponse;
import com.acme.budgeting.v1.CalculateBudgetRequest;
import com.acme.budgeting.v1.CalculateBudgetResponse;
import com.acme.budgeting.v1.CompareBudgetLineOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetLineOptionsResponse;
import com.acme.budgeting.v1.CompareBudgetOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetOptionsResponse;
import com.acme.budgeting.v1.EstimateBudgetLineRequest;
import com.acme.budgeting.v1.EstimateBudgetLineResponse;
import com.acme.budgeting.v1.EstimateBudgetRequest;
import com.acme.budgeting.v1.EstimateBudgetResponse;
import com.acme.budgeting.v1.ExplainBudgetCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetCalculationResponse;
import com.acme.budgeting.v1.ExplainBudgetLineCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetLineCalculationResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.GetBudgetLineRatesRequest;
import com.acme.budgeting.v1.GetBudgetLineRatesResponse;
import com.acme.budgeting.v1.QuoteBudgetLineRequest;
import com.acme.budgeting.v1.QuoteBudgetLineResponse;
import com.acme.budgeting.v1.QuoteBudgetRequest;
import com.acme.budgeting.v1.QuoteBudgetResponse;
import com.acme.budgeting.v1.RecalculateBudgetLinesRequest;
import com.acme.budgeting.v1.RecalculateBudgetLinesResponse;
import com.acme.budgeting.v1.SimulateBudgetLineRequest;
import com.acme.budgeting.v1.SimulateBudgetLineResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.BatchGetDocumentsRequest;
import com.acme.documents.v1.BatchGetDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsRequest;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.recommendations.v1.RecommendationSetQueryServiceGrpc;
import com.acme.recommendations.v1.SummarizeRecommendationSetsRequest;
import com.acme.recommendations.v1.SummarizeRecommendationSetsResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SummarizeSearchIndexesRequest;
import com.acme.search.v1.SummarizeSearchIndexesResponse;
import com.acme.suppliers.v1.SupplierRef;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class BudgetLineCalculationApplicationTest {
    @Test
    void recalculateBudgetLinesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new BudgetLineCalculationApplication(
                DepreciationQueryServiceGrpc.newFutureStub(channel),
                FixedAssetServiceGrpc.newBlockingStub(channel),
                BudgetCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                RecommendationSetQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RecalculateBudgetLinesRequest.getDefaultInstance());
        var response = application.recalculateBudgetLines(request);
        var sent0 = channel.request("acme.assets.v1.DepreciationQueryService/ResolveDepreciations");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        var sent1 = channel.request("acme.recommendations.v1.RecommendationSetQueryService/SummarizeRecommendationSets");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent1, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent1, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "metadata"),
                ProbeSupport.value(sent1, "details", "filters"));
        var sent2 = channel.request("acme.ledger.v1.PostingBatchNotificationService/ListPostingBatchSubscriptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        var sent3 = channel.request("acme.budgeting.v1.BudgetCalculationService/QuoteBudget");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "billing_address", "country_code"),
                ProbeSupport.value(sent3, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "audit", "updated_at"),
                ProbeSupport.value(sent3, "budget", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent3, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "billing_address", "locality"),
                ProbeSupport.value(sent3, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationQueryService/ResolveDepreciations"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationQueryService/ResolveDepreciations"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchNotificationService/ListPostingBatchSubscriptions"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetCalculationService/QuoteBudget"), "breakdown", "total"),
                ProbeSupport.value(response, "breakdown", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line_id"),
                ProbeSupport.value(response, "budget_line", "budget_line_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "total"),
                ProbeSupport.value(response, "budget_line", "total"));
    }
}
