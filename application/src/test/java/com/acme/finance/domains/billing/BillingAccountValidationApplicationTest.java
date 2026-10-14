package com.acme.finance.domains.billing;

import com.acme.banking.v1.NormalizeStandingOrderRequest;
import com.acme.banking.v1.NormalizeStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.CheckBillingAccountEligibilityRequest;
import com.acme.billing.v1.CheckBillingAccountEligibilityResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.EstimateInvoiceRequest;
import com.acme.billing.v1.EstimateInvoiceResponse;
import com.acme.billing.v1.ExplainBillingAccountDecisionRequest;
import com.acme.billing.v1.ExplainBillingAccountDecisionResponse;
import com.acme.billing.v1.ExplainInvoiceCalculationRequest;
import com.acme.billing.v1.ExplainInvoiceCalculationResponse;
import com.acme.billing.v1.GetBillingAccountRulesRequest;
import com.acme.billing.v1.GetBillingAccountRulesResponse;
import com.acme.billing.v1.GetInvoiceRatesRequest;
import com.acme.billing.v1.GetInvoiceRatesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceBankSettlement;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceCardSettlement;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.LintBillingAccountsRequest;
import com.acme.billing.v1.LintBillingAccountsResponse;
import com.acme.billing.v1.NormalizeBillingAccountRequest;
import com.acme.billing.v1.NormalizeBillingAccountResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PreviewBillingAccountRequest;
import com.acme.billing.v1.PreviewBillingAccountResponse;
import com.acme.billing.v1.RecalculateInvoicesRequest;
import com.acme.billing.v1.RecalculateInvoicesResponse;
import com.acme.billing.v1.ScoreBillingAccountRequest;
import com.acme.billing.v1.ScoreBillingAccountResponse;
import com.acme.billing.v1.SimulateInvoiceRequest;
import com.acme.billing.v1.SimulateInvoiceResponse;
import com.acme.billing.v1.ValidateBillingAccountRequest;
import com.acme.billing.v1.ValidateBillingAccountResponse;
import com.acme.budgeting.v1.ValidateVarianceRequest;
import com.acme.budgeting.v1.ValidateVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentCardSettlement;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.LintDocumentsRequest;
import com.acme.documents.v1.LintDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchBankSettlement;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.google.protobuf.Any;
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

class BillingAccountValidationApplicationTest {
    @Test
    void explainBillingAccountDecisionPreservesPlannedFields() {
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
        var application = new BillingAccountValidationApplication(
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                InvoiceCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceCalculationServiceGrpc.newFutureStub(channel),
                VarianceValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                new BillingAccountValidationRequestMapperImpl(),
                repository,
                Runnable::run,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExplainBillingAccountDecisionRequest.getDefaultInstance());
        var response = application.explainBillingAccountDecision(request);
        var sent0 = channel.request("acme.billing.v1.InvoiceCalculationService/SimulateInvoice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "limits", "amount"),
                ProbeSupport.value(sent0, "invoice", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "holder", "display_name"),
                ProbeSupport.value(sent0, "invoice", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "audit", "updated_by"),
                ProbeSupport.value(sent0, "invoice", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "audit", "created_by"),
                ProbeSupport.value(sent0, "invoice", "audit", "created_by"));
        var sent1 = channel.request("acme.billing.v1.InvoiceCalculationService/EstimateInvoice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "limits", "amount"),
                ProbeSupport.value(sent1, "invoice", "lines", "amount"));
        var sent2 = channel.request("acme.banking.v1.StandingOrderValidationService/NormalizeStandingOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "phone"),
                ProbeSupport.value(sent2, "standing_order", "participants", "contact", "phone"));
        var sent3 = channel.request("acme.billing.v1.InvoiceCalculationService/GetInvoiceRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "holder", "invoice_id"),
                ProbeSupport.value(sent3, "invoice_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/NormalizeStandingOrder"), "standing_order", "audit", "created_by"),
                ProbeSupport.value(response, "billing_account", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceCalculationService/GetInvoiceRates"), "invoice", "audit", "updated_by"),
                ProbeSupport.value(response, "billing_account", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceCalculationService/GetInvoiceRates"), "invoice", "counterparty", "display_name"),
                ProbeSupport.value(response, "billing_account", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account_id"),
                ProbeSupport.value(response, "billing_account", "billing_account_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "contact"),
                ProbeSupport.value(response, "billing_account", "contact"));
    }
}
