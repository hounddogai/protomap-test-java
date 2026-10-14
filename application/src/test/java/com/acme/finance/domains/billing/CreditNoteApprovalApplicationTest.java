package com.acme.finance.domains.billing;

import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.BatchGetAccessPoliciesRequest;
import com.acme.authorization.v1.BatchGetAccessPoliciesResponse;
import com.acme.billing.v1.ApproveCreditNoteRequest;
import com.acme.billing.v1.ApproveCreditNoteResponse;
import com.acme.billing.v1.BatchCreateBillingAccountsRequest;
import com.acme.billing.v1.BatchCreateBillingAccountsResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountLimit;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.billing.v1.BillingAccountWorkflowServiceGrpc;
import com.acme.billing.v1.CommentCreditNoteApprovalRequest;
import com.acme.billing.v1.CommentCreditNoteApprovalResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteLine;
import com.acme.billing.v1.CreditNoteLineTax;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.GetCreditNoteApprovalStatusRequest;
import com.acme.billing.v1.GetCreditNoteApprovalStatusResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceApprovalServiceGrpc;
import com.acme.billing.v1.InvoiceCardSettlement;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.ListPendingCreditNotesRequest;
import com.acme.billing.v1.ListPendingCreditNotesResponse;
import com.acme.billing.v1.NormalizeCreditNoteRequest;
import com.acme.billing.v1.NormalizeCreditNoteResponse;
import com.acme.billing.v1.PauseBillingAccountWorkflowRequest;
import com.acme.billing.v1.PauseBillingAccountWorkflowResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.RecallInvoiceRequest;
import com.acme.billing.v1.RecallInvoiceResponse;
import com.acme.billing.v1.RejectBillingAccountRequest;
import com.acme.billing.v1.RejectBillingAccountResponse;
import com.acme.billing.v1.RejectCreditNoteRequest;
import com.acme.billing.v1.RejectCreditNoteResponse;
import com.acme.billing.v1.SubmitCreditNoteRequest;
import com.acme.billing.v1.SubmitCreditNoteResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.ExplainHouseholdDecisionRequest;
import com.acme.customers.v1.ExplainHouseholdDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.QuoteCustomerRequest;
import com.acme.customers.v1.QuoteCustomerResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.FindDocumentsRequest;
import com.acme.documents.v1.FindDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryCardSettlement;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.ValidateJournalEntryRequest;
import com.acme.ledger.v1.ValidateJournalEntryResponse;
import java.util.ArrayList;
import java.util.List;
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

class CreditNoteApprovalApplicationTest {
    @Test
    void submitCreditNotePreservesPlannedFields() {
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
        var application = new CreditNoteApprovalApplication(
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                BillingAccountApprovalServiceGrpc.newBlockingStub(channel),
                BillingAccountServiceGrpc.newStub(channel),
                BillingAccountWorkflowServiceGrpc.newBlockingStub(channel),
                CreditNoteValidationServiceGrpc.newBlockingStub(channel),
                InvoiceApprovalServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                DocumentQueryServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newStub(channel),
                JournalEntryValidationServiceGrpc.newBlockingStub(channel),
                new CreditNoteApprovalRequestMapperImpl(),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(SubmitCreditNoteRequest.getDefaultInstance());
        var response = application.submitCreditNote(request);
        var sent0 = channel.request("acme.billing.v1.CreditNoteValidationService/NormalizeCreditNote");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "lines", "tax", "exemptions"),
                ProbeSupport.value(sent0, "credit_note", "lines", "tax", "exemptions"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "lines", "tax", "amount"),
                ProbeSupport.value(sent0, "credit_note", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "lines", "unit_price"),
                ProbeSupport.value(sent0, "credit_note", "lines", "unit_price"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "lines", "attributes"),
                ProbeSupport.value(sent0, "credit_note", "lines", "attributes"));
        var sent1 = channel.request("acme.billing.v1.BillingAccountWorkflowService/PauseBillingAccountWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "lines", "tax", "amount"),
                ProbeSupport.value(sent1, "billing_account", "limits", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "card", "token"),
                ProbeSupport.value(sent1, "billing_account", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteValidationService/NormalizeCreditNote"), "credit_note", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "billing_account", "holder", "display_name"));
        var sent2 = channel.request("acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteValidationService/NormalizeCreditNote"), "credit_note", "audit", "created_by"),
                ProbeSupport.value(sent2, "household", "children", "children", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteValidationService/NormalizeCreditNote"), "credit_note", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "household", "children", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note_id"),
                ProbeSupport.value(response, "credit_note", "credit_note_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "billing_address"),
                ProbeSupport.value(response, "credit_note", "billing_address"));
    }
}
