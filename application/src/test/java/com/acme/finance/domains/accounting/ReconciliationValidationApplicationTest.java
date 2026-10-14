package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.CalculateReconciliationRequest;
import com.acme.accounting.v1.CalculateReconciliationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.CheckAccrualEligibilityRequest;
import com.acme.accounting.v1.CheckAccrualEligibilityResponse;
import com.acme.accounting.v1.CheckReconciliationEligibilityRequest;
import com.acme.accounting.v1.CheckReconciliationEligibilityResponse;
import com.acme.accounting.v1.CompareAllocationOptionsRequest;
import com.acme.accounting.v1.CompareAllocationOptionsResponse;
import com.acme.accounting.v1.EstimateAllocationRequest;
import com.acme.accounting.v1.EstimateAllocationResponse;
import com.acme.accounting.v1.ExplainChartOfAccountsDecisionRequest;
import com.acme.accounting.v1.ExplainChartOfAccountsDecisionResponse;
import com.acme.accounting.v1.ExplainReconciliationDecisionRequest;
import com.acme.accounting.v1.ExplainReconciliationDecisionResponse;
import com.acme.accounting.v1.GetReconciliationRulesRequest;
import com.acme.accounting.v1.GetReconciliationRulesResponse;
import com.acme.accounting.v1.LintReconciliationsRequest;
import com.acme.accounting.v1.LintReconciliationsResponse;
import com.acme.accounting.v1.NormalizeChartOfAccountsRequest;
import com.acme.accounting.v1.NormalizeChartOfAccountsResponse;
import com.acme.accounting.v1.NormalizeReconciliationRequest;
import com.acme.accounting.v1.NormalizeReconciliationResponse;
import com.acme.accounting.v1.PreviewReconciliationRequest;
import com.acme.accounting.v1.PreviewReconciliationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationComment;
import com.acme.accounting.v1.ReconciliationSla;
import com.acme.accounting.v1.ScoreReconciliationRequest;
import com.acme.accounting.v1.ScoreReconciliationResponse;
import com.acme.accounting.v1.SimulateReconciliationRequest;
import com.acme.accounting.v1.SimulateReconciliationResponse;
import com.acme.accounting.v1.ValidateChartOfAccountsRequest;
import com.acme.accounting.v1.ValidateChartOfAccountsResponse;
import com.acme.accounting.v1.ValidateReconciliationRequest;
import com.acme.accounting.v1.ValidateReconciliationResponse;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.DisposalSla;
import com.acme.assets.v1.QuoteDisposalRequest;
import com.acme.assets.v1.QuoteDisposalResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.SummarizeBankAccountsRequest;
import com.acme.banking.v1.SummarizeBankAccountsResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.PreviewBillingAccountRequest;
import com.acme.billing.v1.PreviewBillingAccountResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.dispatch.v1.CreateDispatchOrderRequest;
import com.acme.dispatch.v1.CreateDispatchOrderResponse;
import com.acme.dispatch.v1.DispatchOrder;
import com.acme.dispatch.v1.DispatchOrderBankSettlement;
import com.acme.dispatch.v1.DispatchOrderServiceGrpc;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.LintDocumentsRequest;
import com.acme.documents.v1.LintDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.localization.v1.ResolveTranslationKeysRequest;
import com.acme.localization.v1.ResolveTranslationKeysResponse;
import com.acme.localization.v1.TranslationKeyQueryServiceGrpc;
import com.acme.notifications.v1.CreateNotificationRequest;
import com.acme.notifications.v1.CreateNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionResponse;
import com.acme.procurement.v1.NormalizeRfqRequest;
import com.acme.procurement.v1.NormalizeRfqResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import java.util.List;
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

class ReconciliationValidationApplicationTest {
    @Test
    void normalizeReconciliationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var listener = new ReconciliationValidationApplicationEvents(DisposalCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ReconciliationValidationApplication.NormalizeReconciliationStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ReconciliationValidationApplication(
                AccrualValidationServiceGrpc.newBlockingStub(channel),
                AllocationCalculationServiceGrpc.newBlockingStub(channel),
                ChartOfAccountsValidationServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                BillingAccountValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                DispatchOrderServiceGrpc.newBlockingStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                events,
                TranslationKeyQueryServiceGrpc.newBlockingStub(channel),
                new ReconciliationValidationRequestMapperImpl(),
                NotificationServiceGrpc.newBlockingStub(channel),
                PurchaseOrderValidationServiceGrpc.newBlockingStub(channel),
                RfqValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(NormalizeReconciliationRequest.getDefaultInstance());
        var response = application.normalizeReconciliation(request);
        var sent0 = channel.request("acme.procurement.v1.PurchaseOrderValidationService/ExplainPurchaseOrderDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "audit", "etag"),
                ProbeSupport.value(sent0, "purchase_order", "audit", "etag"));
        var sent1 = channel.request("acme.accounting.v1.ChartOfAccountsValidationService/ValidateChartOfAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderValidationService/ExplainPurchaseOrderDecision"), "purchase_order", "audit", "created_at"),
                ProbeSupport.value(sent1, "chart_of_accounts", "children", "children", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "customer", "chart_of_accounts_id"),
                ProbeSupport.value(sent1, "chart_of_accounts", "children", "children", "children", "chart_of_accounts_id"));
        var sent2 = channel.request("acme.assets.v1.DisposalCalculationService/QuoteDisposal");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsValidationService/ValidateChartOfAccounts"), "chart_of_accounts", "audit", "updated_by"),
                ProbeSupport.value(sent2, "disposal", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "audit", "created_at"),
                ProbeSupport.value(sent2, "disposal", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderValidationService/ExplainPurchaseOrderDecision"), "purchase_order", "billing_address", "region"),
                ProbeSupport.value(sent2, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "sla", "due_at"),
                ProbeSupport.value(sent2, "disposal", "sla", "due_at"));
        var sent3 = channel.request("acme.localization.v1.TranslationKeyQueryService/ResolveTranslationKeys");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderValidationService/ExplainPurchaseOrderDecision"), "purchase_order", "audit", "created_at"),
                ProbeSupport.value(response, "reconciliation", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderValidationService/ExplainPurchaseOrderDecision"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ChartOfAccountsValidationService/ValidateChartOfAccounts"), "chart_of_accounts", "manager", "display_name"),
                ProbeSupport.value(response, "reconciliation", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalCalculationService/QuoteDisposal"), "disposal", "sla", "breached"),
                ProbeSupport.value(response, "reconciliation", "sla", "breached"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.localization.v1.TranslationKeyQueryService/ResolveTranslationKeys"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation_id"),
                ProbeSupport.value(response, "reconciliation", "reconciliation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reconciliation", "subject"),
                ProbeSupport.value(response, "reconciliation", "subject"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ReconciliationValidationApplicationEvents(DisposalCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<QuoteDisposalResponse>();
        listener.on(new ReconciliationValidationApplication.NormalizeReconciliationStage2Requested(ProbeSupport.sample(QuoteDisposalRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
