package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.RetryAuditTrailExportRequest;
import com.acme.audit.v1.RetryAuditTrailExportResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.NormalizeBudgetLineRequest;
import com.acme.budgeting.v1.NormalizeBudgetLineResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.QuoteClaimRequest;
import com.acme.insurance.v1.QuoteClaimResponse;
import com.acme.invoicing.v1.BrowseInvoiceBatchesRequest;
import com.acme.invoicing.v1.BrowseInvoiceBatchesResponse;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityResponse;
import com.acme.invoicing.v1.CountInvoiceBatchesRequest;
import com.acme.invoicing.v1.CountInvoiceBatchesResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceValidationServiceGrpc;
import com.acme.invoicing.v1.ExplainInvoiceTemplateDecisionRequest;
import com.acme.invoicing.v1.ExplainInvoiceTemplateDecisionResponse;
import com.acme.invoicing.v1.ExportInvoiceBatchesRequest;
import com.acme.invoicing.v1.ExportInvoiceBatchesResponse;
import com.acme.invoicing.v1.FindInvoiceBatchesRequest;
import com.acme.invoicing.v1.FindInvoiceBatchesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateLine;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.LookupInvoiceBatchRequest;
import com.acme.invoicing.v1.LookupInvoiceBatchResponse;
import com.acme.invoicing.v1.NormalizeInvoiceBatchRequest;
import com.acme.invoicing.v1.NormalizeInvoiceBatchResponse;
import com.acme.invoicing.v1.PeekInvoiceBatchRequest;
import com.acme.invoicing.v1.PeekInvoiceBatchResponse;
import com.acme.invoicing.v1.ResolveInvoiceBatchesRequest;
import com.acme.invoicing.v1.ResolveInvoiceBatchesResponse;
import com.acme.invoicing.v1.SimulateInvoiceTemplateRequest;
import com.acme.invoicing.v1.SimulateInvoiceTemplateResponse;
import com.acme.invoicing.v1.SuggestInvoiceBatchesRequest;
import com.acme.invoicing.v1.SuggestInvoiceBatchesResponse;
import com.acme.invoicing.v1.ValidateEInvoiceRequest;
import com.acme.invoicing.v1.ValidateEInvoiceResponse;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.SuggestRiskRegistersRequest;
import com.acme.risk.v1.SuggestRiskRegistersResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.search.v1.ValidateSearchIndexRequest;
import com.acme.search.v1.ValidateSearchIndexResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class InvoiceBatchQueryApplicationTest {
    @Test
    void exportInvoiceBatchesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var listener = new InvoiceBatchQueryApplicationEvents(EInvoiceValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof InvoiceBatchQueryApplication.ExportInvoiceBatchesStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new InvoiceBatchQueryApplication(
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                BudgetLineValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                InvoiceBatchValidationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateValidationServiceGrpc.newBlockingStub(channel),
                new InvoiceBatchQueryRequestMapperImpl(),
                repository,
                RiskRegisterQueryServiceGrpc.newBlockingStub(channel),
                SearchIndexValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExportInvoiceBatchesRequest.getDefaultInstance());
        var response = application.exportInvoiceBatches(request);
        var sent0 = channel.request("acme.invoicing.v1.EInvoiceValidationService/ValidateEInvoice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "e_invoice", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        var sent1 = channel.request("acme.risk.v1.RiskRegisterQueryService/SuggestRiskRegisters");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceValidationService/ValidateEInvoice"), "e_invoice", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        var sent2 = channel.request("acme.search.v1.SearchIndexValidationService/ValidateSearchIndex");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceValidationService/ValidateEInvoice"), "e_invoice", "billing_address", "locality"),
                ProbeSupport.value(sent2, "search_index", "address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceValidationService/ValidateEInvoice"), "e_invoice", "billing_address", "region"),
                ProbeSupport.value(sent2, "search_index", "address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceValidationService/ValidateEInvoice"), "e_invoice", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "search_index", "address", "country_code"));
        var sent3 = channel.request("acme.insurance.v1.ClaimCalculationService/QuoteClaim");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.search.v1.SearchIndexValidationService/ValidateSearchIndex"), "search_index", "owner", "display_name"),
                ProbeSupport.value(sent3, "claim", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceValidationService/ValidateEInvoice"), "e_invoice", "billing_address", "country_code"),
                ProbeSupport.value(sent3, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceValidationService/ValidateEInvoice"), "e_invoice", "billing_address", "locality"),
                ProbeSupport.value(sent3, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceValidationService/ValidateEInvoice"), "e_invoice", "billing_address"),
                ProbeSupport.value(response, "invoice_batch", "billing_address"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceValidationService/ValidateEInvoice"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterQueryService/SuggestRiskRegisters"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterQueryService/SuggestRiskRegisters"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/QuoteClaim"), "claim", "customer", "email"),
                ProbeSupport.value(response, "invoice_batch", "counterparty", "email"));
        var endpoint = new InvoiceBatchQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportInvoiceBatchesResponse> observer = new StreamObserver<>() {
            public void onNext(ExportInvoiceBatchesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportInvoiceBatches(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new InvoiceBatchQueryApplicationEvents(EInvoiceValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ValidateEInvoiceResponse>();
        listener.on(new InvoiceBatchQueryApplication.ExportInvoiceBatchesStage0Requested(ProbeSupport.sample(ValidateEInvoiceRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
