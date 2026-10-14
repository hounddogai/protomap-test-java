package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualApprovalServiceGrpc;
import com.acme.accounting.v1.AccrualLine;
import com.acme.accounting.v1.AccrualLineTax;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.ApproveAccrualRequest;
import com.acme.accounting.v1.ApproveAccrualResponse;
import com.acme.accounting.v1.ApproveAllocationRequest;
import com.acme.accounting.v1.ApproveAllocationResponse;
import com.acme.accounting.v1.CalculateAllocationRequest;
import com.acme.accounting.v1.CalculateAllocationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.accounting.v1.CommentAllocationApprovalRequest;
import com.acme.accounting.v1.CommentAllocationApprovalResponse;
import com.acme.accounting.v1.DelegateAllocationApprovalRequest;
import com.acme.accounting.v1.DelegateAllocationApprovalResponse;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalRequest;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalResponse;
import com.acme.accounting.v1.GetAllocationApprovalStatusRequest;
import com.acme.accounting.v1.GetAllocationApprovalStatusResponse;
import com.acme.accounting.v1.LintReconciliationsRequest;
import com.acme.accounting.v1.LintReconciliationsResponse;
import com.acme.accounting.v1.ListChartOfAccountsesRequest;
import com.acme.accounting.v1.ListChartOfAccountsesResponse;
import com.acme.accounting.v1.ListPendingAllocationsRequest;
import com.acme.accounting.v1.ListPendingAllocationsResponse;
import com.acme.accounting.v1.RecallAccrualRequest;
import com.acme.accounting.v1.RecallAccrualResponse;
import com.acme.accounting.v1.RecallAllocationRequest;
import com.acme.accounting.v1.RecallAllocationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.accounting.v1.SimulateAllocationRequest;
import com.acme.accounting.v1.SimulateAllocationResponse;
import com.acme.accounting.v1.SubmitAllocationRequest;
import com.acme.accounting.v1.SubmitAllocationResponse;
import com.acme.accounting.v1.ValidateReconciliationRequest;
import com.acme.accounting.v1.ValidateReconciliationResponse;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyCondition;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.collections.v1.ApproveCollectionCaseRequest;
import com.acme.collections.v1.ApproveCollectionCaseResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.CoverageCondition;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.RejectCoverageRequest;
import com.acme.insurance.v1.RejectCoverageResponse;
import com.acme.insurance.v1.SchedulePremiumReportRequest;
import com.acme.insurance.v1.SchedulePremiumReportResponse;
import com.acme.insurance.v1.SharePremiumReportRequest;
import com.acme.insurance.v1.SharePremiumReportResponse;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceApprovalServiceGrpc;
import com.acme.invoicing.v1.EInvoiceLine;
import com.acme.invoicing.v1.EInvoiceLineTax;
import com.acme.invoicing.v1.EscalateEInvoiceRequest;
import com.acme.invoicing.v1.EscalateEInvoiceResponse;
import com.acme.invoicing.v1.ListPendingEInvoicesRequest;
import com.acme.invoicing.v1.ListPendingEInvoicesResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
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

class AllocationApprovalApplicationTest {
    @Test
    void delegateAllocationApprovalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new AllocationApprovalApplication(
                AccrualApprovalServiceGrpc.newBlockingStub(channel),
                AllocationCalculationServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsApprovalServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsServiceGrpc.newBlockingStub(channel),
                ReconciliationValidationServiceGrpc.newBlockingStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                BudgetServiceGrpc.newStub(channel),
                CollectionCaseApprovalServiceGrpc.newBlockingStub(channel),
                CoverageApprovalServiceGrpc.newBlockingStub(channel),
                PremiumReportingServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newStub(channel),
                EInvoiceApprovalServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(DelegateAllocationApprovalRequest.getDefaultInstance());
        var response = application.delegateAllocationApproval(request);
        var sent0 = channel.request("acme.insurance.v1.PremiumReportingService/SharePremiumReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent0, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "block", "reason"),
                ProbeSupport.value(sent0, "reason"));
        var sent1 = channel.request("acme.insurance.v1.PremiumReportingService/SchedulePremiumReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "block", "reason"),
                ProbeSupport.value(sent1, "reason"));
        var sent2 = channel.request("acme.accounting.v1.AllocationCalculationService/CalculateAllocation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "owner", "email"),
                ProbeSupport.value(sent2, "allocation", "owner", "email"));
        var sent3 = channel.request("acme.accounting.v1.AccrualApprovalService/RecallAccrual");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationCalculationService/CalculateAllocation"), "allocation", "discount", "amount"),
                ProbeSupport.value(sent3, "accrual", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumReportingService/SharePremiumReport"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumReportingService/SharePremiumReport"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumReportingService/SchedulePremiumReport"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationCalculationService/CalculateAllocation"), "allocation", "audit", "created_by"),
                ProbeSupport.value(response, "allocation", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualApprovalService/RecallAccrual"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualApprovalService/RecallAccrual"), "accrual", "audit", "created_at"),
                ProbeSupport.value(response, "allocation", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation_id"),
                ProbeSupport.value(response, "allocation", "allocation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "allocation", "priority"),
                ProbeSupport.value(response, "allocation", "priority"));
    }
}
