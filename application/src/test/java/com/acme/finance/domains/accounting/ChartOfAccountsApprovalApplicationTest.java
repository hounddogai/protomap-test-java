package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualQueryServiceGrpc;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.ApproveChartOfAccountsRequest;
import com.acme.accounting.v1.ApproveChartOfAccountsResponse;
import com.acme.accounting.v1.BrowseAccrualsRequest;
import com.acme.accounting.v1.BrowseAccrualsResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.CommentChartOfAccountsApprovalRequest;
import com.acme.accounting.v1.CommentChartOfAccountsApprovalResponse;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalRequest;
import com.acme.accounting.v1.DelegateChartOfAccountsApprovalResponse;
import com.acme.accounting.v1.ExplainAllocationDecisionRequest;
import com.acme.accounting.v1.ExplainAllocationDecisionResponse;
import com.acme.accounting.v1.GetChartOfAccountsApprovalStatusRequest;
import com.acme.accounting.v1.GetChartOfAccountsApprovalStatusResponse;
import com.acme.accounting.v1.NormalizeAllocationRequest;
import com.acme.accounting.v1.NormalizeAllocationResponse;
import com.acme.accounting.v1.PreviewAllocationRequest;
import com.acme.accounting.v1.PreviewAllocationResponse;
import com.acme.accounting.v1.RecalculateReconciliationsRequest;
import com.acme.accounting.v1.RecalculateReconciliationsResponse;
import com.acme.accounting.v1.RecallChartOfAccountsRequest;
import com.acme.accounting.v1.RecallChartOfAccountsResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.RejectChartOfAccountsRequest;
import com.acme.accounting.v1.RejectChartOfAccountsResponse;
import com.acme.accounting.v1.ScoreAccrualRequest;
import com.acme.accounting.v1.ScoreAccrualResponse;
import com.acme.accounting.v1.SimulateAllocationRequest;
import com.acme.accounting.v1.SimulateAllocationResponse;
import com.acme.accounting.v1.SubmitChartOfAccountsRequest;
import com.acme.accounting.v1.SubmitChartOfAccountsResponse;
import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.banking.v1.ApproveStandingOrderRequest;
import com.acme.banking.v1.ApproveStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.SubmitStandingOrderRequest;
import com.acme.banking.v1.SubmitStandingOrderResponse;
import com.acme.billing.v1.AdvanceBillingAccountWorkflowRequest;
import com.acme.billing.v1.AdvanceBillingAccountWorkflowResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountWorkflowServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ImportUnderwritingsRequest;
import com.acme.insurance.v1.ImportUnderwritingsResponse;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceApprovalServiceGrpc;
import com.acme.invoicing.v1.RecallEInvoiceRequest;
import com.acme.invoicing.v1.RecallEInvoiceResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsResponse;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanLimit;
import com.acme.localization.v1.ResolveTranslationKeysRequest;
import com.acme.localization.v1.ResolveTranslationKeysResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ChartOfAccountsApprovalApplicationTest {
    @Test
    void rejectChartOfAccountsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var listener = new ChartOfAccountsApprovalApplicationEvents(LoanApprovalServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ChartOfAccountsApprovalApplication.DelegateChartOfAccountsApprovalStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ChartOfAccountsApprovalApplication(
                AccrualQueryServiceGrpc.newBlockingStub(channel),
                AccrualValidationServiceGrpc.newBlockingStub(channel),
                AllocationCalculationServiceGrpc.newBlockingStub(channel),
                AllocationValidationServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                StandingOrderApprovalServiceGrpc.newBlockingStub(channel),
                BillingAccountWorkflowServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                UnderwritingImportServiceGrpc.newStub(channel),
                EInvoiceApprovalServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(RejectChartOfAccountsRequest.getDefaultInstance());
        var response = application.rejectChartOfAccounts(request);
        var sent0 = channel.request("acme.audit.v1.RetentionPolicyHistoryService/RecordRetentionPolicyVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent0, "retention_policy", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "audit", "updated_by"),
                ProbeSupport.value(sent0, "retention_policy", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "display_name"),
                ProbeSupport.value(sent0, "retention_policy", "owner", "display_name"));
        var sent1 = channel.request("acme.accounting.v1.AccrualValidationService/ScoreAccrual");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "location", "postal_code"),
                ProbeSupport.value(sent1, "details", "destination", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "location", "locality"),
                ProbeSupport.value(sent1, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "location", "region"),
                ProbeSupport.value(sent1, "accrual", "billing_address", "region"));
        var sent2 = channel.request("acme.accounting.v1.AllocationValidationService/ExplainAllocationDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyHistoryService/RecordRetentionPolicyVersion"), "retention_policy", "owner", "email"),
                ProbeSupport.value(sent2, "allocation", "owner", "email"));
        var sent3 = channel.request("acme.localization.v1.TranslationKeyQueryService/ResolveTranslationKeys");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyHistoryService/RecordRetentionPolicyVersion"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyHistoryService/RecordRetentionPolicyVersion"), "retention_policy", "owner", "display_name"),
                ProbeSupport.value(response, "chart_of_accounts", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualValidationService/ScoreAccrual"), "accrual", "billing_address", "region"),
                ProbeSupport.value(response, "chart_of_accounts", "children", "location", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualValidationService/ScoreAccrual"), "accrual", "audit", "created_at"),
                ProbeSupport.value(response, "chart_of_accounts", "children", "children", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationValidationService/ExplainAllocationDecision"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.localization.v1.TranslationKeyQueryService/ResolveTranslationKeys"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.localization.v1.TranslationKeyQueryService/ResolveTranslationKeys"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts_id"),
                ProbeSupport.value(response, "chart_of_accounts", "chart_of_accounts_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "chart_of_accounts", "parent_id"),
                ProbeSupport.value(response, "chart_of_accounts", "parent_id"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ChartOfAccountsApprovalApplicationEvents(LoanApprovalServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CommentLoanApprovalResponse>();
        listener.on(new ChartOfAccountsApprovalApplication.DelegateChartOfAccountsApprovalStage1Requested(ProbeSupport.sample(CommentLoanApprovalRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
