package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.AnnotateAuditEventVersionRequest;
import com.acme.audit.v1.AnnotateAuditEventVersionResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.banking.v1.PreviewStandingOrderRequest;
import com.acme.banking.v1.PreviewStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.FindContractsRequest;
import com.acme.contracts.v1.FindContractsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.BrowseFeatureFlagsRequest;
import com.acme.featureflags.v1.BrowseFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.invoicing.v1.CalculateInvoiceBatchRequest;
import com.acme.invoicing.v1.CalculateInvoiceBatchResponse;
import com.acme.invoicing.v1.CalculateInvoiceTemplateRequest;
import com.acme.invoicing.v1.CalculateInvoiceTemplateResponse;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EstimateInvoiceTemplateRequest;
import com.acme.invoicing.v1.EstimateInvoiceTemplateResponse;
import com.acme.invoicing.v1.ExplainInvoiceTemplateCalculationRequest;
import com.acme.invoicing.v1.ExplainInvoiceTemplateCalculationResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateRatesRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateRatesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchAttachment;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchLine;
import com.acme.invoicing.v1.InvoiceBatchLineTax;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.QuoteInvoiceTemplateRequest;
import com.acme.invoicing.v1.QuoteInvoiceTemplateResponse;
import com.acme.invoicing.v1.RecalculateInvoiceBatchesRequest;
import com.acme.invoicing.v1.RecalculateInvoiceBatchesResponse;
import com.acme.invoicing.v1.SimulateInvoiceBatchRequest;
import com.acme.invoicing.v1.SimulateInvoiceBatchResponse;
import com.acme.invoicing.v1.SimulateInvoiceTemplateRequest;
import com.acme.invoicing.v1.SimulateInvoiceTemplateResponse;
import com.acme.ledger.v1.BrowseJournalEntriesRequest;
import com.acme.ledger.v1.BrowseJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class InvoiceTemplateCalculationApplicationTest {
    @Test
    void calculateInvoiceTemplatePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var listener = new InvoiceTemplateCalculationApplicationEvents(JournalEntryQueryServiceGrpc.newStub(channel), PayStubCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof InvoiceTemplateCalculationApplication.EstimateInvoiceTemplateStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof InvoiceTemplateCalculationApplication.ExplainInvoiceTemplateCalculationStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new InvoiceTemplateCalculationApplication(
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                ContractQueryServiceGrpc.newBlockingStub(channel),
                events,
                FeatureFlagQueryServiceGrpc.newBlockingStub(channel),
                InvoiceBatchCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(CalculateInvoiceTemplateRequest.getDefaultInstance());
        var response = application.calculateInvoiceTemplate(request);
        var sent0 = channel.request("acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "counterparty", "customer_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.invoicing.v1.InvoiceBatchCalculationService/CalculateInvoiceBatch");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "lines", "tax", "jurisdiction"),
                ProbeSupport.value(sent1, "invoice_batch", "lines", "tax", "jurisdiction"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "lines", "tax", "rate"),
                ProbeSupport.value(sent1, "invoice_batch", "lines", "tax", "rate"));
        var sent2 = channel.request("acme.audit.v1.AuditEventHistoryService/AnnotateAuditEventVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "audit_event", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/CalculateInvoiceBatch"), "result", "correlation_id"),
                ProbeSupport.value(sent2, "audit_event", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "number"),
                ProbeSupport.value(sent2, "audit_event", "subject", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent2, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/AnnotateAuditEventVersion"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template_id"),
                ProbeSupport.value(response, "invoice_template", "invoice_template_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "metadata"),
                ProbeSupport.value(response, "invoice_template", "metadata"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new InvoiceTemplateCalculationApplicationEvents(JournalEntryQueryServiceGrpc.newStub(channel), PayStubCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<EstimatePayStubResponse>();
        listener.on(new InvoiceTemplateCalculationApplication.EstimateInvoiceTemplateStage1Requested(ProbeSupport.sample(EstimatePayStubRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<BrowseJournalEntriesResponse>();
        listener.on(new InvoiceTemplateCalculationApplication.ExplainInvoiceTemplateCalculationStage0Requested(ProbeSupport.sample(BrowseJournalEntriesRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
