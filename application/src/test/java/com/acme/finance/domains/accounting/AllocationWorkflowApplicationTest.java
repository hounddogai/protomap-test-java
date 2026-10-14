package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualApprovalServiceGrpc;
import com.acme.accounting.v1.AccrualLine;
import com.acme.accounting.v1.AccrualLineTax;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.AdvanceAllocationWorkflowRequest;
import com.acme.accounting.v1.AdvanceAllocationWorkflowResponse;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationApprovalServiceGrpc;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.CalculateAllocationRequest;
import com.acme.accounting.v1.CalculateAllocationResponse;
import com.acme.accounting.v1.CancelAllocationWorkflowRequest;
import com.acme.accounting.v1.CancelAllocationWorkflowResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.CompareAllocationOptionsRequest;
import com.acme.accounting.v1.CompareAllocationOptionsResponse;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalRequest;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalResponse;
import com.acme.accounting.v1.GetAllocationApprovalStatusRequest;
import com.acme.accounting.v1.GetAllocationApprovalStatusResponse;
import com.acme.accounting.v1.InspectAllocationWorkflowRequest;
import com.acme.accounting.v1.InspectAllocationWorkflowResponse;
import com.acme.accounting.v1.ListAllocationWorkflowsRequest;
import com.acme.accounting.v1.ListAllocationWorkflowsResponse;
import com.acme.accounting.v1.ListChartOfAccountsesRequest;
import com.acme.accounting.v1.ListChartOfAccountsesResponse;
import com.acme.accounting.v1.ListPendingAllocationsRequest;
import com.acme.accounting.v1.ListPendingAllocationsResponse;
import com.acme.accounting.v1.PauseAllocationWorkflowRequest;
import com.acme.accounting.v1.PauseAllocationWorkflowResponse;
import com.acme.accounting.v1.PreviewAccrualRequest;
import com.acme.accounting.v1.PreviewAccrualResponse;
import com.acme.accounting.v1.RecallAccrualRequest;
import com.acme.accounting.v1.RecallAccrualResponse;
import com.acme.accounting.v1.RecallChartOfAccountsRequest;
import com.acme.accounting.v1.RecallChartOfAccountsResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationApprovalServiceGrpc;
import com.acme.accounting.v1.ReconciliationLifecycleServiceGrpc;
import com.acme.accounting.v1.RejectReconciliationRequest;
import com.acme.accounting.v1.RejectReconciliationResponse;
import com.acme.accounting.v1.ResumeAllocationWorkflowRequest;
import com.acme.accounting.v1.ResumeAllocationWorkflowResponse;
import com.acme.accounting.v1.RetryAllocationWorkflowRequest;
import com.acme.accounting.v1.RetryAllocationWorkflowResponse;
import com.acme.accounting.v1.ScheduleReconciliationExpiryRequest;
import com.acme.accounting.v1.ScheduleReconciliationExpiryResponse;
import com.acme.accounting.v1.ScoreChartOfAccountsRequest;
import com.acme.accounting.v1.ScoreChartOfAccountsResponse;
import com.acme.accounting.v1.SignalAllocationWorkflowRequest;
import com.acme.accounting.v1.SignalAllocationWorkflowResponse;
import com.acme.accounting.v1.StartAllocationWorkflowRequest;
import com.acme.accounting.v1.StartAllocationWorkflowResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetWorkflowServiceGrpc;
import com.acme.assets.v1.ResumeFixedAssetWorkflowRequest;
import com.acme.assets.v1.ResumeFixedAssetWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.SummarizeDocumentsRequest;
import com.acme.documents.v1.SummarizeDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.FindUsersRequest;
import com.acme.identity.v1.FindUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesRequest;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesResponse;
import com.acme.metrics.v1.CloneMetricSeriesRequest;
import com.acme.metrics.v1.CloneMetricSeriesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderApprovalServiceGrpc;
import com.acme.procurement.v1.RecallPurchaseOrderRequest;
import com.acme.procurement.v1.RecallPurchaseOrderResponse;
import com.google.protobuf.Any;
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

class AllocationWorkflowApplicationTest {
    @Test
    void resumeAllocationWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var listener = new AllocationWorkflowApplicationEvents(FixedAssetWorkflowServiceGrpc.newStub(channel), DocumentQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof AllocationWorkflowApplication.StartAllocationWorkflowStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof AllocationWorkflowApplication.ResumeAllocationWorkflowStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new AllocationWorkflowApplication(
                AccrualApprovalServiceGrpc.newBlockingStub(channel),
                AccrualValidationServiceGrpc.newBlockingStub(channel),
                AllocationApprovalServiceGrpc.newBlockingStub(channel),
                AllocationCalculationServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsApprovalServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsValidationServiceGrpc.newBlockingStub(channel),
                ReconciliationApprovalServiceGrpc.newBlockingStub(channel),
                ReconciliationLifecycleServiceGrpc.newBlockingStub(channel),
                events,
                UserQueryServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                new AllocationWorkflowRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                PurchaseOrderApprovalServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ResumeAllocationWorkflowRequest.getDefaultInstance());
        var response = application.resumeAllocationWorkflow(request);
        var sent0 = channel.request("acme.accounting.v1.ReconciliationLifecycleService/ScheduleReconciliationExpiry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "audit", "updated_at"),
                ProbeSupport.value(sent0, "reconciliation", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "audit", "created_at"),
                ProbeSupport.value(sent0, "reconciliation", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "notify", "preferred_channel"));
        var sent1 = channel.request("acme.documents.v1.DocumentQueryService/SummarizeDocuments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "block", "reason"),
                ProbeSupport.value(sent1, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent1, "annotations"));
        var sent2 = channel.request("acme.accounting.v1.ChartOfAccountsService/ListChartOfAccountses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "owner", "user_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationLifecycleService/ScheduleReconciliationExpiry"), "result", "details"),
                ProbeSupport.value(sent2, "filter", "labels"));
        var sent3 = channel.request("acme.accounting.v1.AllocationApprovalService/ListPendingAllocations");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationLifecycleService/ScheduleReconciliationExpiry"), "reconciliation", "audit", "created_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationLifecycleService/ScheduleReconciliationExpiry"), "reconciliation", "customer", "chart_of_accounts_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "parameters"),
                ProbeSupport.value(sent3, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationLifecycleService/ScheduleReconciliationExpiry"), "reconciliation", "assignee", "display_name"),
                ProbeSupport.value(response, "allocation", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentQueryService/SummarizeDocuments"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsService/ListChartOfAccountses"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsService/ListChartOfAccountses"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation_id"),
                ProbeSupport.value(response, "allocation", "allocation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "version"),
                ProbeSupport.value(response, "allocation", "version"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new AllocationWorkflowApplicationEvents(FixedAssetWorkflowServiceGrpc.newStub(channel), DocumentQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ResumeFixedAssetWorkflowResponse>();
        listener.on(new AllocationWorkflowApplication.StartAllocationWorkflowStage1Requested(ProbeSupport.sample(ResumeFixedAssetWorkflowRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<SummarizeDocumentsResponse>();
        listener.on(new AllocationWorkflowApplication.ResumeAllocationWorkflowStage1Requested(ProbeSupport.sample(SummarizeDocumentsRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
