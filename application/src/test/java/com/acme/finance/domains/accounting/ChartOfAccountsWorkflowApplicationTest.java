package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualApprovalServiceGrpc;
import com.acme.accounting.v1.AdvanceChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.AdvanceChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.CancelChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.CancelChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.accounting.v1.CheckReconciliationEligibilityRequest;
import com.acme.accounting.v1.CheckReconciliationEligibilityResponse;
import com.acme.accounting.v1.InspectChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.InspectChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.ListChartOfAccountsWorkflowsRequest;
import com.acme.accounting.v1.ListChartOfAccountsWorkflowsResponse;
import com.acme.accounting.v1.NormalizeAllocationRequest;
import com.acme.accounting.v1.NormalizeAllocationResponse;
import com.acme.accounting.v1.NormalizeReconciliationRequest;
import com.acme.accounting.v1.NormalizeReconciliationResponse;
import com.acme.accounting.v1.PauseChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.PauseChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.RecallAccrualRequest;
import com.acme.accounting.v1.RecallAccrualResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.accounting.v1.ResumeChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.ResumeChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.RetryChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.RetryChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.ScoreReconciliationRequest;
import com.acme.accounting.v1.ScoreReconciliationResponse;
import com.acme.accounting.v1.SignalChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.SignalChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.StartChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.StartChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.SubmitChartOfAccountsRequest;
import com.acme.accounting.v1.SubmitChartOfAccountsResponse;
import com.acme.accounting.v1.UpdateChartOfAccountsRequest;
import com.acme.accounting.v1.UpdateChartOfAccountsResponse;
import com.acme.accounting.v1.ValidateAllocationRequest;
import com.acme.accounting.v1.ValidateAllocationResponse;
import com.acme.accounting.v1.ValidateReconciliationRequest;
import com.acme.accounting.v1.ValidateReconciliationResponse;
import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.ResolveAuditTrailsRequest;
import com.acme.audit.v1.ResolveAuditTrailsResponse;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.GeneratePremiumReportRequest;
import com.acme.insurance.v1.GeneratePremiumReportResponse;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.PullUnderwritingsRequest;
import com.acme.insurance.v1.PullUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.SuggestJournalEntriesRequest;
import com.acme.ledger.v1.SuggestJournalEntriesResponse;
import com.acme.risk.v1.RetryRiskScenarioWorkflowRequest;
import com.acme.risk.v1.RetryRiskScenarioWorkflowResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class ChartOfAccountsWorkflowApplicationTest {
    @Test
    void cancelChartOfAccountsWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new ChartOfAccountsWorkflowApplication(
                AccrualApprovalServiceGrpc.newBlockingStub(channel),
                AllocationValidationServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsApprovalServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsServiceGrpc.newBlockingStub(channel),
                ReconciliationValidationServiceGrpc.newBlockingStub(channel),
                AuditTrailQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PremiumReportingServiceGrpc.newBlockingStub(channel),
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                new ChartOfAccountsWorkflowRequestMapperImpl(),
                repository,
                RiskScenarioWorkflowServiceGrpc.newBlockingStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(CancelChartOfAccountsWorkflowRequest.getDefaultInstance());
        var response = application.cancelChartOfAccountsWorkflow(request);
        var sent0 = channel.request("acme.accounting.v1.AllocationValidationService/NormalizeAllocation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "manager", "display_name"),
                ProbeSupport.value(sent0, "allocation", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "manager", "user_id"),
                ProbeSupport.value(sent0, "allocation", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "audit", "updated_by"),
                ProbeSupport.value(sent0, "allocation", "audit", "updated_by"));
        var sent1 = channel.request("acme.accounting.v1.ReconciliationValidationService/ScoreReconciliation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationValidationService/NormalizeAllocation"), "allocation", "owner", "email"),
                ProbeSupport.value(sent1, "reconciliation", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent1, "reconciliation", "requester", "email"));
        var sent2 = channel.request("acme.accounting.v1.ReconciliationValidationService/NormalizeReconciliation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationValidationService/ScoreReconciliation"), "reconciliation", "customer", "path"),
                ProbeSupport.value(sent2, "reconciliation", "customer", "path"));
        var sent3 = channel.request("acme.accounting.v1.AccrualApprovalService/RecallAccrual");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationValidationService/NormalizeAllocation"), "allocation", "audit", "etag"),
                ProbeSupport.value(sent3, "accrual", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationValidationService/NormalizeReconciliation"), "reconciliation", "assignee", "email"),
                ProbeSupport.value(sent3, "details", "approver", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "location", "locality"),
                ProbeSupport.value(sent3, "accrual", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationValidationService/NormalizeAllocation"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationValidationService/NormalizeAllocation"), "allocation", "owner", "email"),
                ProbeSupport.value(response, "chart_of_accounts", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationValidationService/ScoreReconciliation"), "reconciliation", "audit", "etag"),
                ProbeSupport.value(response, "chart_of_accounts", "children", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationValidationService/NormalizeReconciliation"), "reconciliation", "audit", "created_at"),
                ProbeSupport.value(response, "chart_of_accounts", "children", "children", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationValidationService/NormalizeReconciliation"), "reconciliation", "audit", "created_by"),
                ProbeSupport.value(response, "chart_of_accounts", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualApprovalService/RecallAccrual"), "accrual", "audit", "created_at"),
                ProbeSupport.value(response, "chart_of_accounts", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualApprovalService/RecallAccrual"), "accrual", "audit", "etag"),
                ProbeSupport.value(response, "chart_of_accounts", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts_id"),
                ProbeSupport.value(response, "chart_of_accounts", "chart_of_accounts_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "location"),
                ProbeSupport.value(response, "chart_of_accounts", "location"));
    }
}
