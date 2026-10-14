package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.ResolveAuditTrailsRequest;
import com.acme.audit.v1.ResolveAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.NormalizeComplianceCaseRequest;
import com.acme.compliance.v1.NormalizeComplianceCaseResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.BatchCreateExpenseReportsRequest;
import com.acme.expenses.v1.BatchCreateExpenseReportsResponse;
import com.acme.expenses.v1.ExpenseReportServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.insurance.v1.ComparePremiumOptionsRequest;
import com.acme.insurance.v1.ComparePremiumOptionsResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PremiumLineTax;
import com.acme.invoicing.v1.CheckEInvoiceEligibilityRequest;
import com.acme.invoicing.v1.CheckEInvoiceEligibilityResponse;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityResponse;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityResponse;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceAttachment;
import com.acme.invoicing.v1.EInvoiceCalculationServiceGrpc;
import com.acme.invoicing.v1.ExplainEInvoiceDecisionRequest;
import com.acme.invoicing.v1.ExplainEInvoiceDecisionResponse;
import com.acme.invoicing.v1.GetEInvoiceRulesRequest;
import com.acme.invoicing.v1.GetEInvoiceRulesResponse;
import com.acme.invoicing.v1.GetInvoiceBatchRatesRequest;
import com.acme.invoicing.v1.GetInvoiceBatchRatesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchBankSettlement;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchCardSettlement;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateBankSettlement;
import com.acme.invoicing.v1.InvoiceTemplateCardSettlement;
import com.acme.invoicing.v1.InvoiceTemplateLine;
import com.acme.invoicing.v1.InvoiceTemplateLineTax;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.LintEInvoicesRequest;
import com.acme.invoicing.v1.LintEInvoicesResponse;
import com.acme.invoicing.v1.LintInvoiceTemplatesRequest;
import com.acme.invoicing.v1.LintInvoiceTemplatesResponse;
import com.acme.invoicing.v1.NormalizeEInvoiceRequest;
import com.acme.invoicing.v1.NormalizeEInvoiceResponse;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateRequest;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateResponse;
import com.acme.invoicing.v1.PreviewEInvoiceRequest;
import com.acme.invoicing.v1.PreviewEInvoiceResponse;
import com.acme.invoicing.v1.PreviewInvoiceTemplateRequest;
import com.acme.invoicing.v1.PreviewInvoiceTemplateResponse;
import com.acme.invoicing.v1.QuoteInvoiceBatchRequest;
import com.acme.invoicing.v1.QuoteInvoiceBatchResponse;
import com.acme.invoicing.v1.ScoreEInvoiceRequest;
import com.acme.invoicing.v1.ScoreEInvoiceResponse;
import com.acme.invoicing.v1.ScoreInvoiceBatchRequest;
import com.acme.invoicing.v1.ScoreInvoiceBatchResponse;
import com.acme.invoicing.v1.ScoreInvoiceTemplateRequest;
import com.acme.invoicing.v1.ScoreInvoiceTemplateResponse;
import com.acme.invoicing.v1.SimulateEInvoiceRequest;
import com.acme.invoicing.v1.SimulateEInvoiceResponse;
import com.acme.invoicing.v1.ValidateEInvoiceRequest;
import com.acme.invoicing.v1.ValidateEInvoiceResponse;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.treasury.v1.BankTransferQueryServiceGrpc;
import com.acme.treasury.v1.ResolveBankTransfersRequest;
import com.acme.treasury.v1.ResolveBankTransfersResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class EInvoiceValidationApplicationTest {
    @Test
    void lintEInvoicesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var listener = new EInvoiceValidationApplicationEvents(InvoiceBatchCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof EInvoiceValidationApplication.NormalizeEInvoiceStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new EInvoiceValidationApplication(
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                AuditTrailQueryServiceGrpc.newFutureStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                events,
                ExpenseReportServiceGrpc.newStub(channel),
                PremiumCalculationServiceGrpc.newBlockingStub(channel),
                EInvoiceCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchValidationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateValidationServiceGrpc.newBlockingStub(channel),
                LedgerAccountQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                BankTransferQueryServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(LintEInvoicesRequest.getDefaultInstance());
        var response = application.lintEInvoices(request);
        var sent0 = channel.request("acme.invoicing.v1.InvoiceTemplateValidationService/NormalizeInvoiceTemplate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "bank", "account_holder"),
                ProbeSupport.value(sent0, "invoice_template", "bank", "account_holder"));
        var sent1 = channel.request("acme.audit.v1.AuditTrailExportService/ListAuditTrailExports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/NormalizeInvoiceTemplate"), "invoice_template", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        var sent2 = channel.request("acme.invoicing.v1.InvoiceTemplateValidationService/PreviewInvoiceTemplate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "card", "last4"),
                ProbeSupport.value(sent2, "invoice_template", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "email"),
                ProbeSupport.value(sent2, "invoice_template", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/NormalizeInvoiceTemplate"), "invoice_template", "card", "token"),
                ProbeSupport.value(sent2, "invoice_template", "card", "token"));
        var sent3 = channel.request("acme.invoicing.v1.InvoiceTemplateValidationService/CheckInvoiceTemplateEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/PreviewInvoiceTemplate"), "invoice_template", "audit", "created_by"),
                ProbeSupport.value(sent3, "invoice_template", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/NormalizeInvoiceTemplate"), "invoice_template", "bank", "account_holder"),
                ProbeSupport.value(sent3, "invoice_template", "bank", "account_holder"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/PreviewInvoiceTemplate"), "invoice_template", "counterparty", "email"),
                ProbeSupport.value(sent3, "invoice_template", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/NormalizeInvoiceTemplate"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/NormalizeInvoiceTemplate"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailExportService/ListAuditTrailExports"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/PreviewInvoiceTemplate"), "invoice_template", "number"),
                ProbeSupport.value(response, "e_invoice", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/CheckInvoiceTemplateEligibility"), "invoice_template", "counterparty", "display_name"),
                ProbeSupport.value(response, "e_invoice", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice_id"),
                ProbeSupport.value(response, "e_invoice", "e_invoice_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "number"),
                ProbeSupport.value(response, "e_invoice", "counterparty", "number"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new EInvoiceValidationApplicationEvents(InvoiceBatchCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<QuoteInvoiceBatchResponse>();
        listener.on(new EInvoiceValidationApplication.NormalizeEInvoiceStage0Requested(ProbeSupport.sample(QuoteInvoiceBatchRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
