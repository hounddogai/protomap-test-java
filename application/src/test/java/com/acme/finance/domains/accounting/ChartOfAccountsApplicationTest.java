package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualQueryServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesResponse;
import com.acme.accounting.v1.BatchGetChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchGetChartOfAccountsesResponse;
import com.acme.accounting.v1.BrowseAccrualsRequest;
import com.acme.accounting.v1.BrowseAccrualsResponse;
import com.acme.accounting.v1.CalculateReconciliationRequest;
import com.acme.accounting.v1.CalculateReconciliationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.CompareAllocationOptionsRequest;
import com.acme.accounting.v1.CompareAllocationOptionsResponse;
import com.acme.accounting.v1.CreateChartOfAccountsRequest;
import com.acme.accounting.v1.CreateChartOfAccountsResponse;
import com.acme.accounting.v1.DeleteChartOfAccountsRequest;
import com.acme.accounting.v1.DeleteChartOfAccountsResponse;
import com.acme.accounting.v1.EstimateAllocationRequest;
import com.acme.accounting.v1.EstimateAllocationResponse;
import com.acme.accounting.v1.ExplainReconciliationCalculationRequest;
import com.acme.accounting.v1.ExplainReconciliationCalculationResponse;
import com.acme.accounting.v1.GetChartOfAccountsRequest;
import com.acme.accounting.v1.GetChartOfAccountsResponse;
import com.acme.accounting.v1.ListChartOfAccountsesRequest;
import com.acme.accounting.v1.ListChartOfAccountsesResponse;
import com.acme.accounting.v1.PreviewReconciliationRequest;
import com.acme.accounting.v1.PreviewReconciliationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.accounting.v1.ScoreChartOfAccountsRequest;
import com.acme.accounting.v1.ScoreChartOfAccountsResponse;
import com.acme.accounting.v1.SuggestAccrualsRequest;
import com.acme.accounting.v1.SuggestAccrualsResponse;
import com.acme.accounting.v1.UpdateChartOfAccountsRequest;
import com.acme.accounting.v1.UpdateChartOfAccountsResponse;
import com.acme.accounting.v1.WatchChartOfAccountsesRequest;
import com.acme.accounting.v1.WatchChartOfAccountsesResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.PreviewFixedAssetRequest;
import com.acme.assets.v1.PreviewFixedAssetResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CloneAccessPolicyRequest;
import com.acme.authorization.v1.CloneAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.LintPostingBatchesRequest;
import com.acme.ledger.v1.LintPostingBatchesResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class ChartOfAccountsApplicationTest {
    @Test
    void updateChartOfAccountsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new ChartOfAccountsApplication(
                AccrualQueryServiceGrpc.newBlockingStub(channel),
                AllocationCalculationServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsValidationServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newFutureStub(channel),
                ReconciliationValidationServiceGrpc.newFutureStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ComplianceCaseServiceGrpc.newStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                new ChartOfAccountsRequestMapperImpl(),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(UpdateChartOfAccountsRequest.getDefaultInstance());
        var response = application.updateChartOfAccounts(request);
        var sent0 = channel.request("acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "location", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "manager", "user_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "attributes"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.assets.v1.FixedAssetValidationService/PreviewFixedAsset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "location", "postal_code"),
                ProbeSupport.value(sent1, "fixed_asset", "address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "manager", "email"),
                ProbeSupport.value(sent1, "fixed_asset", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "location", "country_code"),
                ProbeSupport.value(sent1, "fixed_asset", "address", "country_code"));
        var sent2 = channel.request("acme.accounting.v1.AllocationCalculationService/EstimateAllocation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "manager", "display_name"),
                ProbeSupport.value(sent2, "allocation", "owner", "display_name"));
        var sent3 = channel.request("acme.accounting.v1.AllocationCalculationService/CompareAllocationOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "audit", "updated_by"),
                ProbeSupport.value(sent3, "allocation", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/PreviewFixedAsset"), "fixed_asset", "owner", "email"),
                ProbeSupport.value(sent3, "allocation", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/PreviewFixedAsset"), "fixed_asset", "address", "recipient"),
                ProbeSupport.value(response, "chart_of_accounts", "location", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationCalculationService/EstimateAllocation"), "allocation", "audit", "etag"),
                ProbeSupport.value(response, "chart_of_accounts", "children", "children", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "parent_id"),
                ProbeSupport.value(response, "chart_of_accounts", "parent_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "manager"),
                ProbeSupport.value(response, "chart_of_accounts", "manager"));
    }

    @Test
    void watchChartOfAccountsesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new ChartOfAccountsApplication(
                AccrualQueryServiceGrpc.newBlockingStub(channel),
                AllocationCalculationServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsValidationServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newFutureStub(channel),
                ReconciliationValidationServiceGrpc.newFutureStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ComplianceCaseServiceGrpc.newStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                new ChartOfAccountsRequestMapperImpl(),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(WatchChartOfAccountsesRequest.getDefaultInstance());
        var response = application.watchChartOfAccountses(request);
        var sent0 = channel.request("acme.authorization.v1.AccessPolicyService/CloneAccessPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "access_policy", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "access_policy", "owner", "user_id"));
        var sent1 = channel.request("acme.accounting.v1.ChartOfAccountsValidationService/ScoreChartOfAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyService/CloneAccessPolicy"), "access_policy", "owner", "email"),
                ProbeSupport.value(sent1, "chart_of_accounts", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyService/CloneAccessPolicy"), "access_policy", "owner", "user_id"),
                ProbeSupport.value(sent1, "chart_of_accounts", "children", "children", "manager", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyService/CloneAccessPolicy"), "access_policy", "audit", "created_by"),
                ProbeSupport.value(sent1, "chart_of_accounts", "children", "children", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyService/CloneAccessPolicy"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyService/CloneAccessPolicy"), "access_policy", "audit", "updated_by"),
                ProbeSupport.value(response, "chart_of_accounts", "children", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsValidationService/ScoreChartOfAccounts"), "chart_of_accounts", "audit", "created_by"),
                ProbeSupport.value(response, "chart_of_accounts", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "chart_of_accounts", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "chart_of_accounts", "location", "region"));
        var endpoint = new ChartOfAccountsEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<WatchChartOfAccountsesResponse> observer = new StreamObserver<>() {
            public void onNext(WatchChartOfAccountsesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.watchChartOfAccountses(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void batchCreateChartOfAccountsesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new ChartOfAccountsApplication(
                AccrualQueryServiceGrpc.newBlockingStub(channel),
                AllocationCalculationServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsValidationServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newFutureStub(channel),
                ReconciliationValidationServiceGrpc.newFutureStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ComplianceCaseServiceGrpc.newStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                new ChartOfAccountsRequestMapperImpl(),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(BatchCreateChartOfAccountsesRequest.getDefaultInstance());
        var response = application.batchCreateChartOfAccountses(request);
        var sent0 = channel.request("acme.ledger.v1.PostingBatchValidationService/LintPostingBatches");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "updated_at"),
                ProbeSupport.value(sent0, "posting_batch", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "manager", "display_name"),
                ProbeSupport.value(sent0, "posting_batch", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchValidationService/LintPostingBatches"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchValidationService/LintPostingBatches"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        var endpoint = new ChartOfAccountsEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreateChartOfAccountsesResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreateChartOfAccountsesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreateChartOfAccountses(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
