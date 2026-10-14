package com.acme.finance.domains.invoicing;

import com.acme.anomalies.v1.Anomaly;
import com.acme.anomalies.v1.AnomalyComment;
import com.acme.anomalies.v1.AnomalyServiceGrpc;
import com.acme.anomalies.v1.CreateAnomalyRequest;
import com.acme.anomalies.v1.CreateAnomalyResponse;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsRequest;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsResponse;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.EstimateReceiptRequest;
import com.acme.expenses.v1.EstimateReceiptResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.CalculateInvoiceBatchRequest;
import com.acme.invoicing.v1.CalculateInvoiceBatchResponse;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityResponse;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsResponse;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EstimateInvoiceBatchRequest;
import com.acme.invoicing.v1.EstimateInvoiceBatchResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchDecisionRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchDecisionResponse;
import com.acme.invoicing.v1.GetInvoiceBatchRatesRequest;
import com.acme.invoicing.v1.GetInvoiceBatchRatesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchAttachment;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchCardSettlement;
import com.acme.invoicing.v1.InvoiceBatchLine;
import com.acme.invoicing.v1.InvoiceBatchLineTax;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateBankSettlement;
import com.acme.invoicing.v1.InvoiceTemplateCalculationServiceGrpc;
import com.acme.invoicing.v1.LintInvoiceBatchesRequest;
import com.acme.invoicing.v1.LintInvoiceBatchesResponse;
import com.acme.invoicing.v1.NormalizeInvoiceBatchRequest;
import com.acme.invoicing.v1.NormalizeInvoiceBatchResponse;
import com.acme.invoicing.v1.PreviewInvoiceBatchRequest;
import com.acme.invoicing.v1.PreviewInvoiceBatchResponse;
import com.acme.invoicing.v1.QuoteInvoiceBatchRequest;
import com.acme.invoicing.v1.QuoteInvoiceBatchResponse;
import com.acme.invoicing.v1.RecalculateInvoiceBatchesRequest;
import com.acme.invoicing.v1.RecalculateInvoiceBatchesResponse;
import com.acme.invoicing.v1.ScoreInvoiceBatchRequest;
import com.acme.invoicing.v1.ScoreInvoiceBatchResponse;
import com.acme.invoicing.v1.SimulateInvoiceBatchRequest;
import com.acme.invoicing.v1.SimulateInvoiceBatchResponse;
import com.acme.invoicing.v1.ValidateInvoiceBatchRequest;
import com.acme.invoicing.v1.ValidateInvoiceBatchResponse;
import com.acme.procurement.v1.PurchaseRequisitionQueryServiceGrpc;
import com.acme.procurement.v1.SummarizePurchaseRequisitionsRequest;
import com.acme.procurement.v1.SummarizePurchaseRequisitionsResponse;
import com.acme.search.v1.NormalizeSearchIndexRequest;
import com.acme.search.v1.NormalizeSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import com.acme.venues.v1.SearchVenuesRequest;
import com.acme.venues.v1.SearchVenuesResponse;
import com.acme.venues.v1.VenueServiceGrpc;
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

class InvoiceBatchValidationApplicationTest {
    @Test
    void scoreInvoiceBatchPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var application = new InvoiceBatchValidationApplication(
                AnomalyServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ReceiptCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateCalculationServiceGrpc.newBlockingStub(channel),
                new InvoiceBatchValidationRequestMapperImpl(),
                PurchaseRequisitionQueryServiceGrpc.newFutureStub(channel),
                repository,
                Runnable::run,
                SearchIndexServiceGrpc.newBlockingStub(channel),
                SearchIndexValidationServiceGrpc.newBlockingStub(channel),
                VenueServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ScoreInvoiceBatchRequest.getDefaultInstance());
        var response = application.scoreInvoiceBatch(request);
        var sent0 = channel.request("acme.invoicing.v1.InvoiceBatchCalculationService/RecalculateInvoiceBatches");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "billing_address", "locality"),
                ProbeSupport.value(sent0, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "billing_address", "lines"),
                ProbeSupport.value(sent0, "invoice_batch", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent0, "invoice_batch", "billing_address", "region"));
        var sent1 = channel.request("acme.invoicing.v1.InvoiceBatchCalculationService/SimulateInvoiceBatch");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "billing_address", "postal_code"),
                ProbeSupport.value(sent1, "details", "destination", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/RecalculateInvoiceBatches"), "invoice_batch", "billing_address", "locality"),
                ProbeSupport.value(sent1, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/RecalculateInvoiceBatches"), "invoice_batch", "card", "holder_name"),
                ProbeSupport.value(sent1, "invoice_batch", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/RecalculateInvoiceBatches"), "invoice_batch", "billing_address", "lines"),
                ProbeSupport.value(sent1, "details", "destination", "lines"));
        var sent2 = channel.request("acme.invoicing.v1.InvoiceBatchCalculationService/GetInvoiceBatchRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch_id"),
                ProbeSupport.value(sent2, "invoice_batch_id"));
        var sent3 = channel.request("acme.invoicing.v1.InvoiceBatchCalculationService/CalculateInvoiceBatch");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/GetInvoiceBatchRates"), "invoice_batch", "billing_address", "locality"),
                ProbeSupport.value(sent3, "invoice_batch", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "counterparty", "customer_id"),
                ProbeSupport.value(sent3, "invoice_batch", "counterparty", "customer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "card", "last4"),
                ProbeSupport.value(sent3, "invoice_batch", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/RecalculateInvoiceBatches"), "invoice_batch", "audit", "etag"),
                ProbeSupport.value(response, "invoice_batch", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/SimulateInvoiceBatch"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/SimulateInvoiceBatch"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/GetInvoiceBatchRates"), "invoice_batch", "total"),
                ProbeSupport.value(response, "invoice_batch", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/CalculateInvoiceBatch"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/CalculateInvoiceBatch"), "invoice_batch", "number"),
                ProbeSupport.value(response, "invoice_batch", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch_id"),
                ProbeSupport.value(response, "invoice_batch", "invoice_batch_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "total"),
                ProbeSupport.value(response, "breakdown", "total"));
    }
}
