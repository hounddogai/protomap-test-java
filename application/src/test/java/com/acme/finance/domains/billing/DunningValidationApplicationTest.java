package com.acme.finance.domains.billing;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountBankFunding;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CheckDunningEligibilityRequest;
import com.acme.billing.v1.CheckDunningEligibilityResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteBankSettlement;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningCalculationServiceGrpc;
import com.acme.billing.v1.DunningComment;
import com.acme.billing.v1.DunningSla;
import com.acme.billing.v1.EstimateCreditNoteRequest;
import com.acme.billing.v1.EstimateCreditNoteResponse;
import com.acme.billing.v1.EstimateDunningRequest;
import com.acme.billing.v1.EstimateDunningResponse;
import com.acme.billing.v1.ExplainDunningDecisionRequest;
import com.acme.billing.v1.ExplainDunningDecisionResponse;
import com.acme.billing.v1.ExplainPaymentTermDecisionRequest;
import com.acme.billing.v1.ExplainPaymentTermDecisionResponse;
import com.acme.billing.v1.GetDunningRulesRequest;
import com.acme.billing.v1.GetDunningRulesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.LintDunningsRequest;
import com.acme.billing.v1.LintDunningsResponse;
import com.acme.billing.v1.NormalizeBillingAccountRequest;
import com.acme.billing.v1.NormalizeBillingAccountResponse;
import com.acme.billing.v1.NormalizeCreditNoteRequest;
import com.acme.billing.v1.NormalizeCreditNoteResponse;
import com.acme.billing.v1.NormalizeDunningRequest;
import com.acme.billing.v1.NormalizeDunningResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermValidationServiceGrpc;
import com.acme.billing.v1.PreviewDunningRequest;
import com.acme.billing.v1.PreviewDunningResponse;
import com.acme.billing.v1.QuoteDunningRequest;
import com.acme.billing.v1.QuoteDunningResponse;
import com.acme.billing.v1.ScoreDunningRequest;
import com.acme.billing.v1.ScoreDunningResponse;
import com.acme.billing.v1.ScoreInvoiceRequest;
import com.acme.billing.v1.ScoreInvoiceResponse;
import com.acme.billing.v1.SimulateInvoiceRequest;
import com.acme.billing.v1.SimulateInvoiceResponse;
import com.acme.billing.v1.ValidateDunningRequest;
import com.acme.billing.v1.ValidateDunningResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptCardSettlement;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ValidateReceiptRequest;
import com.acme.expenses.v1.ValidateReceiptResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CountPoliciesRequest;
import com.acme.insurance.v1.CountPoliciesResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryLine;
import com.acme.ledger.v1.JournalEntryLineTax;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.localization.v1.ExportTranslationKeysRequest;
import com.acme.localization.v1.ExportTranslationKeysResponse;
import com.acme.localization.v1.TranslationKeyQueryServiceGrpc;
import com.acme.metrics.v1.LintMetricSeriesesRequest;
import com.acme.metrics.v1.LintMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionRequest;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.risk.v1.CalculateStressTestRequest;
import com.acme.risk.v1.CalculateStressTestResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class DunningValidationApplicationTest {
    @Test
    void checkDunningEligibilityPreservesPlannedFields() {
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
        var listener = new DunningValidationApplicationEvents(PaymentTermValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof DunningValidationApplication.GetDunningRulesStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new DunningValidationApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BillingAccountValidationServiceGrpc.newBlockingStub(channel),
                CreditNoteCalculationServiceGrpc.newBlockingStub(channel),
                CreditNoteValidationServiceGrpc.newBlockingStub(channel),
                DunningCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ContractServiceGrpc.newStub(channel),
                events,
                PolicyQueryServiceGrpc.newFutureStub(channel),
                JournalEntryValidationServiceGrpc.newBlockingStub(channel),
                TranslationKeyQueryServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                repository,
                StressTestCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CheckDunningEligibilityRequest.getDefaultInstance());
        var response = application.checkDunningEligibility(request);
        var sent0 = channel.request("acme.billing.v1.InvoiceValidationService/ScoreInvoice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "assignee", "email"),
                ProbeSupport.value(sent0, "invoice", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "audit", "created_by"),
                ProbeSupport.value(sent0, "invoice", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "audit", "updated_at"),
                ProbeSupport.value(sent0, "invoice", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "customer", "display_name"),
                ProbeSupport.value(sent0, "invoice", "counterparty", "display_name"));
        var sent1 = channel.request("acme.expenses.v1.ReceiptValidationService/ValidateReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ScoreInvoice"), "invoice", "audit", "created_by"),
                ProbeSupport.value(sent1, "receipt", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "audit", "updated_at"),
                ProbeSupport.value(sent1, "receipt", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ScoreInvoice"), "invoice", "card", "holder_name"),
                ProbeSupport.value(sent1, "receipt", "card", "holder_name"));
        var sent2 = channel.request("acme.billing.v1.CreditNoteCalculationService/EstimateCreditNote");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "comments", "created_at"),
                ProbeSupport.value(sent2, "credit_note", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/ValidateReceipt"), "receipt", "bank", "account_holder"),
                ProbeSupport.value(sent2, "credit_note", "bank", "account_holder"));
        var sent3 = channel.request("acme.risk.v1.StressTestCalculationService/CalculateStressTest");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.CreditNoteCalculationService/EstimateCreditNote"), "credit_note", "audit", "created_by"),
                ProbeSupport.value(sent3, "stress_test", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ScoreInvoice"), "invoice", "audit", "etag"),
                ProbeSupport.value(response, "dunning", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/ValidateReceipt"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/ValidateReceipt"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestCalculationService/CalculateStressTest"), "stress_test", "audit", "updated_at"),
                ProbeSupport.value(response, "dunning", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning_id"),
                ProbeSupport.value(response, "dunning", "dunning_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "custom_fields"),
                ProbeSupport.value(response, "dunning", "custom_fields"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new DunningValidationApplicationEvents(PaymentTermValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ExplainPaymentTermDecisionResponse>();
        listener.on(new DunningValidationApplication.GetDunningRulesStage0Requested(ProbeSupport.sample(ExplainPaymentTermDecisionRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
