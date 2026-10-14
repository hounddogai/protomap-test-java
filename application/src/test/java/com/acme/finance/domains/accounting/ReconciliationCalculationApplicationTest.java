package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.CalculateReconciliationRequest;
import com.acme.accounting.v1.CalculateReconciliationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityRequest;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityResponse;
import com.acme.accounting.v1.CompareReconciliationOptionsRequest;
import com.acme.accounting.v1.CompareReconciliationOptionsResponse;
import com.acme.accounting.v1.EstimateReconciliationRequest;
import com.acme.accounting.v1.EstimateReconciliationResponse;
import com.acme.accounting.v1.ExplainChartOfAccountsDecisionRequest;
import com.acme.accounting.v1.ExplainChartOfAccountsDecisionResponse;
import com.acme.accounting.v1.ExplainReconciliationCalculationRequest;
import com.acme.accounting.v1.ExplainReconciliationCalculationResponse;
import com.acme.accounting.v1.GetReconciliationRatesRequest;
import com.acme.accounting.v1.GetReconciliationRatesResponse;
import com.acme.accounting.v1.LintChartOfAccountsesRequest;
import com.acme.accounting.v1.LintChartOfAccountsesResponse;
import com.acme.accounting.v1.NormalizeChartOfAccountsRequest;
import com.acme.accounting.v1.NormalizeChartOfAccountsResponse;
import com.acme.accounting.v1.PreviewChartOfAccountsRequest;
import com.acme.accounting.v1.PreviewChartOfAccountsResponse;
import com.acme.accounting.v1.QuoteReconciliationRequest;
import com.acme.accounting.v1.QuoteReconciliationResponse;
import com.acme.accounting.v1.RecalculateReconciliationsRequest;
import com.acme.accounting.v1.RecalculateReconciliationsResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ScoreChartOfAccountsRequest;
import com.acme.accounting.v1.ScoreChartOfAccountsResponse;
import com.acme.accounting.v1.SimulateReconciliationRequest;
import com.acme.accounting.v1.SimulateReconciliationResponse;
import com.acme.assets.v1.BatchGetFixedAssetsRequest;
import com.acme.assets.v1.BatchGetFixedAssetsResponse;
import com.acme.assets.v1.CreateFixedAssetRequest;
import com.acme.assets.v1.CreateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.NormalizeAccessPolicyRequest;
import com.acme.authorization.v1.NormalizeAccessPolicyResponse;
import com.acme.broadcasts.v1.Broadcast;
import com.acme.broadcasts.v1.BroadcastServiceGrpc;
import com.acme.broadcasts.v1.UpdateBroadcastRequest;
import com.acme.broadcasts.v1.UpdateBroadcastResponse;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.SuggestCollectionCasesRequest;
import com.acme.collections.v1.SuggestCollectionCasesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.identity.v1.ValidateUserRequest;
import com.acme.identity.v1.ValidateUserResponse;
import com.acme.insurance.v1.CalculateClaimRequest;
import com.acme.insurance.v1.CalculateClaimResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimChannelOrigin;
import com.acme.notifications.v1.CountNotificationsRequest;
import com.acme.notifications.v1.CountNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.search.v1.CountSearchIndexesRequest;
import com.acme.search.v1.CountSearchIndexesResponse;
import com.acme.search.v1.FindSearchIndexesRequest;
import com.acme.search.v1.FindSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class ReconciliationCalculationApplicationTest {
    @Test
    void simulateReconciliationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new ReconciliationCalculationApplication(
                ChartOfAccountsValidationServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsValidationServiceGrpc.newFutureStub(channel),
                FixedAssetServiceGrpc.newBlockingStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                BroadcastServiceGrpc.newBlockingStub(channel),
                CollectionCaseQueryServiceGrpc.newBlockingStub(channel),
                UserValidationServiceGrpc.newBlockingStub(channel),
                ClaimCalculationServiceGrpc.newBlockingStub(channel),
                new ReconciliationCalculationRequestMapperImpl(),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel),
                SearchIndexQueryServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(SimulateReconciliationRequest.getDefaultInstance());
        var response = application.simulateReconciliation(request);
        var sent0 = channel.request("acme.accounting.v1.ChartOfAccountsValidationService/CheckChartOfAccountsEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "recipient"),
                ProbeSupport.value(sent0, "chart_of_accounts", "children", "children", "location", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "audit", "created_by"),
                ProbeSupport.value(sent0, "chart_of_accounts", "children", "children", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "country_code"),
                ProbeSupport.value(sent0, "chart_of_accounts", "children", "children", "location", "country_code"));
        var sent1 = channel.request("acme.accounting.v1.ChartOfAccountsValidationService/LintChartOfAccountses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsValidationService/CheckChartOfAccountsEligibility"), "chart_of_accounts", "audit", "etag"),
                ProbeSupport.value(sent1, "chart_of_accounts", "children", "children", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "audit", "created_by"),
                ProbeSupport.value(sent1, "chart_of_accounts", "children", "children", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "locality"),
                ProbeSupport.value(sent1, "chart_of_accounts", "children", "children", "location", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "assignee", "user_id"),
                ProbeSupport.value(sent1, "chart_of_accounts", "children", "children", "manager", "user_id"));
        var sent2 = channel.request("acme.search.v1.SearchIndexQueryService/FindSearchIndexes");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsValidationService/CheckChartOfAccountsEligibility"), "chart_of_accounts", "location", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsValidationService/LintChartOfAccountses"), "chart_of_accounts", "attributes"),
                ProbeSupport.value(sent2, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsValidationService/CheckChartOfAccountsEligibility"), "chart_of_accounts", "manager", "display_name"),
                ProbeSupport.value(response, "reconciliation", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsValidationService/CheckChartOfAccountsEligibility"), "chart_of_accounts", "manager", "email"),
                ProbeSupport.value(response, "reconciliation", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsValidationService/LintChartOfAccountses"), "chart_of_accounts", "manager", "user_id"),
                ProbeSupport.value(response, "reconciliation", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsValidationService/LintChartOfAccountses"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.search.v1.SearchIndexQueryService/FindSearchIndexes"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation_id"),
                ProbeSupport.value(response, "reconciliation", "reconciliation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "status"),
                ProbeSupport.value(response, "reconciliation", "status"));
    }
}
