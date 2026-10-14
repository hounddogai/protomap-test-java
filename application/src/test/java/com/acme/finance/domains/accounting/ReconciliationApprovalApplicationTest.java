package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationApprovalServiceGrpc;
import com.acme.accounting.v1.ApproveChartOfAccountsRequest;
import com.acme.accounting.v1.ApproveChartOfAccountsResponse;
import com.acme.accounting.v1.ApproveReconciliationRequest;
import com.acme.accounting.v1.ApproveReconciliationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.accounting.v1.CommentReconciliationApprovalRequest;
import com.acme.accounting.v1.CommentReconciliationApprovalResponse;
import com.acme.accounting.v1.CompareReconciliationOptionsRequest;
import com.acme.accounting.v1.CompareReconciliationOptionsResponse;
import com.acme.accounting.v1.DelegateAllocationApprovalRequest;
import com.acme.accounting.v1.DelegateAllocationApprovalResponse;
import com.acme.accounting.v1.DelegateReconciliationApprovalRequest;
import com.acme.accounting.v1.DelegateReconciliationApprovalResponse;
import com.acme.accounting.v1.EscalateReconciliationRequest;
import com.acme.accounting.v1.EscalateReconciliationResponse;
import com.acme.accounting.v1.GetReconciliationApprovalStatusRequest;
import com.acme.accounting.v1.GetReconciliationApprovalStatusResponse;
import com.acme.accounting.v1.GetReconciliationRatesRequest;
import com.acme.accounting.v1.GetReconciliationRatesResponse;
import com.acme.accounting.v1.LintReconciliationsRequest;
import com.acme.accounting.v1.LintReconciliationsResponse;
import com.acme.accounting.v1.ListChartOfAccountsesRequest;
import com.acme.accounting.v1.ListChartOfAccountsesResponse;
import com.acme.accounting.v1.RecallReconciliationRequest;
import com.acme.accounting.v1.RecallReconciliationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationComment;
import com.acme.accounting.v1.RejectReconciliationRequest;
import com.acme.accounting.v1.RejectReconciliationResponse;
import com.acme.accounting.v1.SubmitReconciliationRequest;
import com.acme.accounting.v1.SubmitReconciliationResponse;
import com.acme.assets.v1.FixedAssetWorkflowServiceGrpc;
import com.acme.assets.v1.ListFixedAssetWorkflowsRequest;
import com.acme.assets.v1.ListFixedAssetWorkflowsResponse;
import com.acme.audit.v1.AnnotateAuditTrailVersionRequest;
import com.acme.audit.v1.AnnotateAuditTrailVersionResponse;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsRequest;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsResponse;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesResponse;
import com.acme.returns.v1.CalculateReturnRequestRequest;
import com.acme.returns.v1.CalculateReturnRequestResponse;
import com.acme.returns.v1.ReturnRequest;
import com.acme.returns.v1.ReturnRequestCalculationServiceGrpc;
import com.acme.telehealth.v1.SearchVideoVisitsRequest;
import com.acme.telehealth.v1.SearchVideoVisitsResponse;
import com.acme.telehealth.v1.VideoVisitServiceGrpc;
import com.acme.treasury.v1.AdvanceCashPositionWorkflowRequest;
import com.acme.treasury.v1.AdvanceCashPositionWorkflowResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionWorkflowServiceGrpc;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.SignalCashPositionWorkflowRequest;
import com.acme.treasury.v1.SignalCashPositionWorkflowResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ReconciliationApprovalApplicationTest {
    @Test
    void escalateReconciliationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var listener = new ReconciliationApprovalApplicationEvents(ChartOfAccountsApprovalServiceGrpc.newStub(channel), FixedAssetWorkflowServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ReconciliationApprovalApplication.EscalateReconciliationStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof ReconciliationApprovalApplication.DelegateReconciliationApprovalStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ReconciliationApprovalApplication(
                AllocationApprovalServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                AuditEventServiceGrpc.newStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                repository,
                ReturnRequestCalculationServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                VideoVisitServiceGrpc.newBlockingStub(channel),
                CashPositionWorkflowServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(EscalateReconciliationRequest.getDefaultInstance());
        var response = application.escalateReconciliation(request);
        var sent0 = channel.request("acme.audit.v1.AuditTrailHistoryService/AnnotateAuditTrailVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "assignee", "display_name"),
                ProbeSupport.value(sent0, "audit_trail", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "audit", "updated_at"),
                ProbeSupport.value(sent0, "audit_trail", "audit", "updated_at"));
        var sent1 = channel.request("acme.accounting.v1.ChartOfAccountsApprovalService/ApproveChartOfAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent1, "chart_of_accounts", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/AnnotateAuditTrailVersion"), "audit_trail", "billing_address", "recipient"),
                ProbeSupport.value(sent1, "chart_of_accounts", "children", "children", "location", "recipient"));
        var sent2 = channel.request("acme.accounting.v1.ReconciliationCalculationService/GetReconciliationRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "reconciliation_id"),
                ProbeSupport.value(sent2, "reconciliation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailHistoryService/AnnotateAuditTrailVersion"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsApprovalService/ApproveChartOfAccounts"), "chart_of_accounts", "manager", "email"),
                ProbeSupport.value(response, "reconciliation", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationCalculationService/GetReconciliationRates"), "reconciliation", "audit", "created_at"),
                ProbeSupport.value(response, "reconciliation", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationCalculationService/GetReconciliationRates"), "reconciliation", "audit", "updated_by"),
                ProbeSupport.value(response, "reconciliation", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation_id"),
                ProbeSupport.value(response, "reconciliation", "reconciliation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "status"),
                ProbeSupport.value(response, "reconciliation", "status"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ReconciliationApprovalApplicationEvents(ChartOfAccountsApprovalServiceGrpc.newStub(channel), FixedAssetWorkflowServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ApproveChartOfAccountsResponse>();
        listener.on(new ReconciliationApprovalApplication.EscalateReconciliationStage1Requested(ProbeSupport.sample(ApproveChartOfAccountsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<ListFixedAssetWorkflowsResponse>();
        listener.on(new ReconciliationApprovalApplication.DelegateReconciliationApprovalStage0Requested(ProbeSupport.sample(ListFixedAssetWorkflowsRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
