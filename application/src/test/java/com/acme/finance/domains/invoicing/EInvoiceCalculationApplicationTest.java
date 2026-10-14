package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ContractClauseQueryServiceGrpc;
import com.acme.contracts.v1.FindContractClausesRequest;
import com.acme.contracts.v1.FindContractClausesResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.CreateFeatureFlagRequest;
import com.acme.featureflags.v1.CreateFeatureFlagResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.featureflags.v1.ListFeatureFlagsRequest;
import com.acme.featureflags.v1.ListFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.CalculateEInvoiceRequest;
import com.acme.invoicing.v1.CalculateEInvoiceResponse;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityResponse;
import com.acme.invoicing.v1.CompareEInvoiceOptionsRequest;
import com.acme.invoicing.v1.CompareEInvoiceOptionsResponse;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsResponse;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EstimateEInvoiceRequest;
import com.acme.invoicing.v1.EstimateEInvoiceResponse;
import com.acme.invoicing.v1.ExplainEInvoiceCalculationRequest;
import com.acme.invoicing.v1.ExplainEInvoiceCalculationResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchDecisionRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchDecisionResponse;
import com.acme.invoicing.v1.ExplainInvoiceTemplateCalculationRequest;
import com.acme.invoicing.v1.ExplainInvoiceTemplateCalculationResponse;
import com.acme.invoicing.v1.GetEInvoiceRatesRequest;
import com.acme.invoicing.v1.GetEInvoiceRatesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchBankSettlement;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchCardSettlement;
import com.acme.invoicing.v1.InvoiceBatchLine;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateCardSettlement;
import com.acme.invoicing.v1.NormalizeInvoiceBatchRequest;
import com.acme.invoicing.v1.NormalizeInvoiceBatchResponse;
import com.acme.invoicing.v1.PreviewInvoiceBatchRequest;
import com.acme.invoicing.v1.PreviewInvoiceBatchResponse;
import com.acme.invoicing.v1.QuoteEInvoiceRequest;
import com.acme.invoicing.v1.QuoteEInvoiceResponse;
import com.acme.invoicing.v1.QuoteInvoiceTemplateRequest;
import com.acme.invoicing.v1.QuoteInvoiceTemplateResponse;
import com.acme.invoicing.v1.RecalculateEInvoicesRequest;
import com.acme.invoicing.v1.RecalculateEInvoicesResponse;
import com.acme.invoicing.v1.SimulateEInvoiceRequest;
import com.acme.invoicing.v1.SimulateEInvoiceResponse;
import com.acme.invoicing.v1.ValidateInvoiceBatchRequest;
import com.acme.invoicing.v1.ValidateInvoiceBatchResponse;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.WatchMetricSeriesesRequest;
import com.acme.metrics.v1.WatchMetricSeriesesResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.search.v1.ValidateSearchIndexRequest;
import com.acme.search.v1.ValidateSearchIndexResponse;
import com.acme.venues.v1.FindVenuesRequest;
import com.acme.venues.v1.FindVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import java.util.List;
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

class EInvoiceCalculationApplicationTest {
    @Test
    void simulateEInvoicePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var listener = new EInvoiceCalculationApplicationEvents(InvoiceBatchValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof EInvoiceCalculationApplication.EstimateEInvoiceStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new EInvoiceCalculationApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                ContractClauseQueryServiceGrpc.newBlockingStub(channel),
                events,
                FeatureFlagServiceGrpc.newBlockingStub(channel),
                InvoiceBatchCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchValidationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateCalculationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodHistoryServiceGrpc.newBlockingStub(channel),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterServiceGrpc.newStub(channel),
                Runnable::run,
                SearchIndexValidationServiceGrpc.newBlockingStub(channel),
                VenueQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SimulateEInvoiceRequest.getDefaultInstance());
        var response = application.simulateEInvoice(request);
        var sent0 = channel.request("acme.invoicing.v1.InvoiceBatchValidationService/NormalizeInvoiceBatch");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "audit", "updated_by"),
                ProbeSupport.value(sent0, "invoice_batch", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "card", "token"),
                ProbeSupport.value(sent0, "invoice_batch", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "billing_address", "recipient"),
                ProbeSupport.value(sent0, "invoice_batch", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "card", "network"),
                ProbeSupport.value(sent0, "invoice_batch", "card", "network"));
        var sent1 = channel.request("acme.invoicing.v1.InvoiceBatchValidationService/PreviewInvoiceBatch");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/NormalizeInvoiceBatch"), "invoice_batch", "bank", "iban"),
                ProbeSupport.value(sent1, "invoice_batch", "bank", "iban"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/NormalizeInvoiceBatch"), "invoice_batch", "audit", "created_at"),
                ProbeSupport.value(sent1, "invoice_batch", "audit", "created_at"));
        var sent2 = channel.request("acme.contracts.v1.ContractClauseQueryService/FindContractClauses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "lines", "line_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "lines", "attributes"),
                ProbeSupport.value(sent2, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/PreviewInvoiceBatch"), "invoice_batch", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/NormalizeInvoiceBatch"), "invoice_batch", "issued_at"),
                ProbeSupport.value(response, "e_invoice", "issued_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/NormalizeInvoiceBatch"), "invoice_batch", "counterparty", "display_name"),
                ProbeSupport.value(response, "e_invoice", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/PreviewInvoiceBatch"), "invoice_batch", "billing_address", "lines"),
                ProbeSupport.value(response, "e_invoice", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice_id"),
                ProbeSupport.value(response, "e_invoice", "e_invoice_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "currency_code"),
                ProbeSupport.value(response, "breakdown", "currency_code"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new EInvoiceCalculationApplicationEvents(InvoiceBatchValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CheckInvoiceBatchEligibilityResponse>();
        listener.on(new EInvoiceCalculationApplication.EstimateEInvoiceStage0Requested(ProbeSupport.sample(CheckInvoiceBatchEligibilityRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
