package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.ListAuditEventExportsRequest;
import com.acme.audit.v1.ListAuditEventExportsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseWorkflowServiceGrpc;
import com.acme.contracts.v1.PauseContractClauseWorkflowRequest;
import com.acme.contracts.v1.PauseContractClauseWorkflowResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.MileageClaimWorkflowServiceGrpc;
import com.acme.expenses.v1.PauseReceiptWorkflowRequest;
import com.acme.expenses.v1.PauseReceiptWorkflowResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptWorkflowServiceGrpc;
import com.acme.expenses.v1.SignalMileageClaimWorkflowRequest;
import com.acme.expenses.v1.SignalMileageClaimWorkflowResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.ActivateInvoiceTemplateRequest;
import com.acme.invoicing.v1.ActivateInvoiceTemplateResponse;
import com.acme.invoicing.v1.CalculateInvoiceBatchRequest;
import com.acme.invoicing.v1.CalculateInvoiceBatchResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.ExpireInvoiceTemplateRequest;
import com.acme.invoicing.v1.ExpireInvoiceTemplateResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateLifecycleRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateLifecycleResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateQueryServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateRequest;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateResponse;
import com.acme.invoicing.v1.ReinstateInvoiceTemplateRequest;
import com.acme.invoicing.v1.ReinstateInvoiceTemplateResponse;
import com.acme.invoicing.v1.RenewInvoiceTemplateRequest;
import com.acme.invoicing.v1.RenewInvoiceTemplateResponse;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesResponse;
import com.acme.invoicing.v1.ScheduleInvoiceTemplateExpiryRequest;
import com.acme.invoicing.v1.ScheduleInvoiceTemplateExpiryResponse;
import com.acme.invoicing.v1.SuspendInvoiceTemplateRequest;
import com.acme.invoicing.v1.SuspendInvoiceTemplateResponse;
import com.acme.invoicing.v1.TerminateInvoiceTemplateRequest;
import com.acme.invoicing.v1.TerminateInvoiceTemplateResponse;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.PreviewLedgerAccountRequest;
import com.acme.ledger.v1.PreviewLedgerAccountResponse;
import com.acme.ledger.v1.ValidateLedgerAccountRequest;
import com.acme.ledger.v1.ValidateLedgerAccountResponse;
import com.acme.ticketing.v1.SuggestTicketsRequest;
import com.acme.ticketing.v1.SuggestTicketsResponse;
import com.acme.ticketing.v1.TicketQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
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

class InvoiceTemplateLifecycleApplicationTest {
    @Test
    void scheduleInvoiceTemplateExpiryPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var listener = new InvoiceTemplateLifecycleApplicationEvents(InvoiceTemplateQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof InvoiceTemplateLifecycleApplication.ExpireInvoiceTemplateStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new InvoiceTemplateLifecycleApplication(
                ChannelRegistry.fixed(channel),
                ContractClauseWorkflowServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                events,
                MileageClaimWorkflowServiceGrpc.newBlockingStub(channel),
                ReceiptWorkflowServiceGrpc.newBlockingStub(channel),
                InvoiceBatchCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateValidationServiceGrpc.newBlockingStub(channel),
                LedgerAccountQueryServiceGrpc.newBlockingStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                new InvoiceTemplateLifecycleRequestMapperImpl(),
                repository,
                Runnable::run,
                TicketQueryServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(ScheduleInvoiceTemplateExpiryRequest.getDefaultInstance());
        var response = application.scheduleInvoiceTemplateExpiry(request);
        var sent0 = channel.request("acme.ticketing.v1.TicketQueryService/SuggestTickets");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.contracts.v1.ContractClauseWorkflowService/PauseContractClauseWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "contract_clause", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent1, "contract_clause", "owner", "email"));
        var sent2 = channel.request("acme.expenses.v1.MileageClaimWorkflowService/SignalMileageClaimWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ticketing.v1.TicketQueryService/SuggestTickets"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ticketing.v1.TicketQueryService/SuggestTickets"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractClauseWorkflowService/PauseContractClauseWorkflow"), "contract_clause", "audit", "etag"),
                ProbeSupport.value(response, "invoice_template", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimWorkflowService/SignalMileageClaimWorkflow"), "mileage_claim", "billing_address", "postal_code"),
                ProbeSupport.value(response, "invoice_template", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template_id"),
                ProbeSupport.value(response, "invoice_template", "invoice_template_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "due_at"),
                ProbeSupport.value(response, "invoice_template", "due_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new InvoiceTemplateLifecycleApplicationEvents(InvoiceTemplateQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ResolveInvoiceTemplatesResponse>();
        listener.on(new InvoiceTemplateLifecycleApplication.ExpireInvoiceTemplateStage0Requested(ProbeSupport.sample(ResolveInvoiceTemplatesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
