package com.acme.finance.domains.billing;

import com.acme.admissions.v1.Admission;
import com.acme.admissions.v1.AdmissionMeasurement;
import com.acme.admissions.v1.AdmissionServiceGrpc;
import com.acme.admissions.v1.CreateAdmissionRequest;
import com.acme.admissions.v1.CreateAdmissionResponse;
import com.acme.admissions.v1.GateScanRef;
import com.acme.admissions.v1.ListAdmissionsRequest;
import com.acme.admissions.v1.ListAdmissionsResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.ValidateAssetTransferRequest;
import com.acme.assets.v1.ValidateAssetTransferResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.WatchBankAccountsRequest;
import com.acme.banking.v1.WatchBankAccountsResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountBankFunding;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CalculateCreditNoteRequest;
import com.acme.billing.v1.CalculateCreditNoteResponse;
import com.acme.billing.v1.CompareCreditNoteOptionsRequest;
import com.acme.billing.v1.CompareCreditNoteOptionsResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteLine;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.EstimateCreditNoteRequest;
import com.acme.billing.v1.EstimateCreditNoteResponse;
import com.acme.billing.v1.ExplainBillingAccountDecisionRequest;
import com.acme.billing.v1.ExplainBillingAccountDecisionResponse;
import com.acme.billing.v1.ExplainCreditNoteCalculationRequest;
import com.acme.billing.v1.ExplainCreditNoteCalculationResponse;
import com.acme.billing.v1.GetCreditNoteRatesRequest;
import com.acme.billing.v1.GetCreditNoteRatesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.LintBillingAccountsRequest;
import com.acme.billing.v1.LintBillingAccountsResponse;
import com.acme.billing.v1.NormalizeBillingAccountRequest;
import com.acme.billing.v1.NormalizeBillingAccountResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.QuoteCreditNoteRequest;
import com.acme.billing.v1.QuoteCreditNoteResponse;
import com.acme.billing.v1.RecalculateCreditNotesRequest;
import com.acme.billing.v1.RecalculateCreditNotesResponse;
import com.acme.billing.v1.ScoreBillingAccountRequest;
import com.acme.billing.v1.ScoreBillingAccountResponse;
import com.acme.billing.v1.SimulateCreditNoteRequest;
import com.acme.billing.v1.SimulateCreditNoteResponse;
import com.acme.billing.v1.ValidateBillingAccountRequest;
import com.acme.billing.v1.ValidateBillingAccountResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsRequest;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsResponse;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.StreamLedgerAccountNoticesRequest;
import com.acme.ledger.v1.StreamLedgerAccountNoticesResponse;
import com.acme.ledger.v1.SummarizeLedgerAccountsRequest;
import com.acme.ledger.v1.SummarizeLedgerAccountsResponse;
import com.acme.payroll.v1.BatchGetPayRunsRequest;
import com.acme.payroll.v1.BatchGetPayRunsResponse;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
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

class CreditNoteCalculationApplicationTest {
    @Test
    void compareCreditNoteOptionsPreservesPlannedFields() {
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
        var application = new CreditNoteCalculationApplication(
                AdmissionServiceGrpc.newBlockingStub(channel),
                AdmissionServiceGrpc.newFutureStub(channel),
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                BankAccountServiceGrpc.newBlockingStub(channel),
                BillingAccountValidationServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                LedgerAccountQueryServiceGrpc.newBlockingStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                PayRunServiceGrpc.newFutureStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(CompareCreditNoteOptionsRequest.getDefaultInstance());
        var response = application.compareCreditNoteOptions(request);
        var sent0 = channel.request("acme.payroll.v1.PayRunService/BatchGetPayRuns");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        var sent1 = channel.request("acme.ledger.v1.LedgerAccountHistoryService/AnnotateLedgerAccountVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "bank", "iban"),
                ProbeSupport.value(sent1, "ledger_account", "bank", "iban"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "ledger_account", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "audit", "created_at"),
                ProbeSupport.value(sent1, "ledger_account", "audit", "created_at"));
        var sent2 = channel.request("acme.ledger.v1.LedgerAccountValidationService/NormalizeLedgerAccount");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/AnnotateLedgerAccountVersion"), "ledger_account", "holder", "posting_batch_id"),
                ProbeSupport.value(sent2, "ledger_account", "holder", "posting_batch_id"));
        var sent3 = channel.request("acme.admissions.v1.AdmissionService/CreateAdmission");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/AnnotateLedgerAccountVersion"), "ledger_account", "holder", "display_name"),
                ProbeSupport.value(sent3, "admission", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/NormalizeLedgerAccount"), "result", "correlation_id"),
                ProbeSupport.value(sent3, "admission", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/AnnotateLedgerAccountVersion"), "ledger_account", "audit", "created_at"),
                ProbeSupport.value(sent3, "admission", "measurements", "observed_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note", "issued_at"),
                ProbeSupport.value(sent3, "admission", "occurred_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunService/BatchGetPayRuns"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/AnnotateLedgerAccountVersion"), "ledger_account", "audit", "created_at"),
                ProbeSupport.value(response, "credit_note", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountHistoryService/AnnotateLedgerAccountVersion"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/NormalizeLedgerAccount"), "ledger_account", "audit", "updated_by"),
                ProbeSupport.value(response, "credit_note", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.admissions.v1.AdmissionService/CreateAdmission"), "admission", "attributes"),
                ProbeSupport.value(response, "credit_note", "lines", "attributes"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "credit_note_id"),
                ProbeSupport.value(response, "credit_note", "credit_note_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "currency_code"),
                ProbeSupport.value(response, "breakdown", "currency_code"));
    }
}
