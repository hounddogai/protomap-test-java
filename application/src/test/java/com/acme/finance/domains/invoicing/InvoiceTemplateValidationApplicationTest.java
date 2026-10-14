package com.acme.finance.domains.invoicing;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferAttachment;
import com.acme.assets.v1.AssetTransferCardSettlement;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.NormalizeAssetTransferRequest;
import com.acme.assets.v1.NormalizeAssetTransferResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.ResolveBankAccountsRequest;
import com.acme.banking.v1.ResolveBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.identity.v1.ValidateUserRequest;
import com.acme.identity.v1.ValidateUserResponse;
import com.acme.invoicing.v1.CalculateEInvoiceRequest;
import com.acme.invoicing.v1.CalculateEInvoiceResponse;
import com.acme.invoicing.v1.CalculateInvoiceBatchRequest;
import com.acme.invoicing.v1.CalculateInvoiceBatchResponse;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceBankSettlement;
import com.acme.invoicing.v1.EInvoiceCalculationServiceGrpc;
import com.acme.invoicing.v1.EstimateEInvoiceRequest;
import com.acme.invoicing.v1.EstimateEInvoiceResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationResponse;
import com.acme.invoicing.v1.ExplainInvoiceTemplateDecisionRequest;
import com.acme.invoicing.v1.ExplainInvoiceTemplateDecisionResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateRulesRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateRulesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchBankSettlement;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.LintInvoiceTemplatesRequest;
import com.acme.invoicing.v1.LintInvoiceTemplatesResponse;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateRequest;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateResponse;
import com.acme.invoicing.v1.PreviewInvoiceTemplateRequest;
import com.acme.invoicing.v1.PreviewInvoiceTemplateResponse;
import com.acme.invoicing.v1.ScoreInvoiceTemplateRequest;
import com.acme.invoicing.v1.ScoreInvoiceTemplateResponse;
import com.acme.invoicing.v1.ValidateInvoiceTemplateRequest;
import com.acme.invoicing.v1.ValidateInvoiceTemplateResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.ValidateJournalEntryRequest;
import com.acme.ledger.v1.ValidateJournalEntryResponse;
import com.acme.lending.v1.BatchGetLoanApplicationsRequest;
import com.acme.lending.v1.BatchGetLoanApplicationsResponse;
import com.acme.lending.v1.CloneLoanApplicationRequest;
import com.acme.lending.v1.CloneLoanApplicationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationLineTax;
import com.acme.lending.v1.LoanApplicationServiceGrpc;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.SearchNotificationsRequest;
import com.acme.notifications.v1.SearchNotificationsResponse;
import com.acme.travel.v1.BrowseTripsRequest;
import com.acme.travel.v1.BrowseTripsResponse;
import com.acme.travel.v1.TripQueryServiceGrpc;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class InvoiceTemplateValidationApplicationTest {
    @Test
    void explainInvoiceTemplateDecisionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var application = new InvoiceTemplateValidationApplication(
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                BankAccountQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                UserValidationServiceGrpc.newBlockingStub(channel),
                EInvoiceCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchCalculationServiceGrpc.newBlockingStub(channel),
                LoanApplicationServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                TripQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExplainInvoiceTemplateDecisionRequest.getDefaultInstance());
        var response = application.explainInvoiceTemplateDecision(request);
        var sent0 = channel.request("acme.identity.v1.UserValidationService/ValidateUser");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "preferred_channel"),
                ProbeSupport.value(sent0, "user", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "billing_address", "recipient"),
                ProbeSupport.value(sent0, "user", "primary_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "billing_address", "country_code"),
                ProbeSupport.value(sent0, "user", "primary_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "audit", "created_at"),
                ProbeSupport.value(sent0, "user", "audit", "created_at"));
        var sent1 = channel.request("acme.notifications.v1.NotificationService/SearchNotifications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/ValidateUser"), "user", "primary_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "audit", "created_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/ValidateUser"), "user", "relationship_manager", "user_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        var sent2 = channel.request("acme.ledger.v1.JournalEntryValidationService/ValidateJournalEntry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/ValidateUser"), "user", "contact", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "phone"),
                ProbeSupport.value(sent2, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/ValidateUser"), "user", "primary_address", "lines"),
                ProbeSupport.value(sent2, "journal_entry", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/ValidateUser"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/ValidateUser"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/SearchNotifications"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryValidationService/ValidateJournalEntry"), "journal_entry", "due_at"),
                ProbeSupport.value(response, "invoice_template", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template_id"),
                ProbeSupport.value(response, "invoice_template", "invoice_template_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "status"),
                ProbeSupport.value(response, "invoice_template", "status"));
    }
}
