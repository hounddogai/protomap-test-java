package com.acme.finance.domains.billing;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferAttachment;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.EstimateAssetTransferRequest;
import com.acme.assets.v1.EstimateAssetTransferResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountBankFunding;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountLimit;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CheckBillingAccountEligibilityRequest;
import com.acme.billing.v1.CheckBillingAccountEligibilityResponse;
import com.acme.billing.v1.CheckCreditNoteEligibilityRequest;
import com.acme.billing.v1.CheckCreditNoteEligibilityResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.ExplainCreditNoteDecisionRequest;
import com.acme.billing.v1.ExplainCreditNoteDecisionResponse;
import com.acme.billing.v1.ExplainInvoiceDecisionRequest;
import com.acme.billing.v1.ExplainInvoiceDecisionResponse;
import com.acme.billing.v1.GetCreditNoteRulesRequest;
import com.acme.billing.v1.GetCreditNoteRulesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceAttachment;
import com.acme.billing.v1.InvoiceBankSettlement;
import com.acme.billing.v1.InvoiceCardSettlement;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.LintBillingAccountsRequest;
import com.acme.billing.v1.LintBillingAccountsResponse;
import com.acme.billing.v1.LintCreditNotesRequest;
import com.acme.billing.v1.LintCreditNotesResponse;
import com.acme.billing.v1.NormalizeCreditNoteRequest;
import com.acme.billing.v1.NormalizeCreditNoteResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PreviewCreditNoteRequest;
import com.acme.billing.v1.PreviewCreditNoteResponse;
import com.acme.billing.v1.PreviewInvoiceRequest;
import com.acme.billing.v1.PreviewInvoiceResponse;
import com.acme.billing.v1.ScoreCreditNoteRequest;
import com.acme.billing.v1.ScoreCreditNoteResponse;
import com.acme.billing.v1.ValidateCreditNoteRequest;
import com.acme.billing.v1.ValidateCreditNoteResponse;
import com.acme.billing.v1.ValidateInvoiceRequest;
import com.acme.billing.v1.ValidateInvoiceResponse;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.FindBudgetsRequest;
import com.acme.budgeting.v1.FindBudgetsResponse;
import com.acme.budgeting.v1.SuggestBudgetsRequest;
import com.acme.budgeting.v1.SuggestBudgetsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ControlQueryServiceGrpc;
import com.acme.compliance.v1.SummarizeControlsRequest;
import com.acme.compliance.v1.SummarizeControlsResponse;
import com.acme.contracts.v1.LintObligationsRequest;
import com.acme.contracts.v1.LintObligationsResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationComment;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.helpdesk.v1.ExplainTicketCalculationRequest;
import com.acme.helpdesk.v1.ExplainTicketCalculationResponse;
import com.acme.helpdesk.v1.Ticket;
import com.acme.helpdesk.v1.TicketCalculationServiceGrpc;
import com.acme.ledger.v1.LintPostingBatchesRequest;
import com.acme.ledger.v1.LintPostingBatchesResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchBankSettlement;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.lending.v1.CalculateLoanRequest;
import com.acme.lending.v1.CalculateLoanResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.notifications.v1.BatchGetNotificationsRequest;
import com.acme.notifications.v1.BatchGetNotificationsResponse;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.ratelimiting.v1.ExportRateLimitsRequest;
import com.acme.ratelimiting.v1.ExportRateLimitsResponse;
import com.acme.ratelimiting.v1.RateLimitQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
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

class CreditNoteValidationApplicationTest {
    @Test
    void previewCreditNotePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BillingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBillingAccount(String tenant, BillingAccount value) {}
            public void saveCreditNote(String tenant, CreditNote value) {}
            public void saveDunning(String tenant, Dunning value) {}
            public void saveInvoice(String tenant, Invoice value) {}
            public void savePaymentTerm(String tenant, PaymentTerm value) {}
        };
        var application = new CreditNoteValidationApplication(
                AssetTransferCalculationServiceGrpc.newBlockingStub(channel),
                BillingAccountValidationServiceGrpc.newBlockingStub(channel),
                InvoiceValidationServiceGrpc.newBlockingStub(channel),
                BudgetQueryServiceGrpc.newBlockingStub(channel),
                ControlQueryServiceGrpc.newBlockingStub(channel),
                ObligationValidationServiceGrpc.newBlockingStub(channel),
                TicketCalculationServiceGrpc.newBlockingStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                LoanCalculationServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                RateLimitQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(PreviewCreditNoteRequest.getDefaultInstance());
        var response = application.previewCreditNote(request);
        var sent0 = channel.request("acme.ratelimiting.v1.RateLimitQueryService/ExportRateLimits");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.contracts.v1.ObligationValidationService/LintObligations");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "obligation", "customer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "audit", "created_at"),
                ProbeSupport.value(sent1, "obligation", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "audit", "created_by"),
                ProbeSupport.value(sent1, "obligation", "audit", "created_by"));
        var sent2 = channel.request("acme.notifications.v1.NotificationService/BatchGetNotifications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        var sent3 = channel.request("acme.helpdesk.v1.TicketCalculationService/ExplainTicketCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationValidationService/LintObligations"), "obligation", "requester", "preferred_channel"),
                ProbeSupport.value(sent3, "ticket", "requester", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationValidationService/LintObligations"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationValidationService/LintObligations"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/BatchGetNotifications"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/BatchGetNotifications"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.helpdesk.v1.TicketCalculationService/ExplainTicketCalculation"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.helpdesk.v1.TicketCalculationService/ExplainTicketCalculation"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note_id"),
                ProbeSupport.value(response, "credit_note", "credit_note_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "number"),
                ProbeSupport.value(response, "credit_note", "number"));
    }
}
