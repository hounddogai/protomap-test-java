package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.PushRetentionPolicyRequest;
import com.acme.audit.v1.PushRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CompareConsentRecordOptionsRequest;
import com.acme.customers.v1.CompareConsentRecordOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.deliveries.v1.BrowseDeliveriesRequest;
import com.acme.deliveries.v1.BrowseDeliveriesResponse;
import com.acme.deliveries.v1.DeliveryQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.CountEInvoiceTotalsRequest;
import com.acme.invoicing.v1.CountEInvoiceTotalsResponse;
import com.acme.invoicing.v1.DeleteEInvoiceReportRequest;
import com.acme.invoicing.v1.DeleteEInvoiceReportResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.FetchEInvoiceReportRequest;
import com.acme.invoicing.v1.FetchEInvoiceReportResponse;
import com.acme.invoicing.v1.GenerateEInvoiceReportRequest;
import com.acme.invoicing.v1.GenerateEInvoiceReportResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.ListEInvoiceReportsRequest;
import com.acme.invoicing.v1.ListEInvoiceReportsResponse;
import com.acme.invoicing.v1.ShareEInvoiceReportRequest;
import com.acme.invoicing.v1.ShareEInvoiceReportResponse;
import com.acme.invoicing.v1.StreamEInvoiceMetricsRequest;
import com.acme.invoicing.v1.StreamEInvoiceMetricsResponse;
import com.acme.keymanagement.v1.CreateCryptoKeyRequest;
import com.acme.keymanagement.v1.CreateCryptoKeyResponse;
import com.acme.keymanagement.v1.CryptoKey;
import com.acme.keymanagement.v1.CryptoKeyBankFunding;
import com.acme.keymanagement.v1.CryptoKeyServiceGrpc;
import com.acme.ledger.v1.ExplainLedgerAccountDecisionRequest;
import com.acme.ledger.v1.ExplainLedgerAccountDecisionResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.sponsorships.v1.ExportSponsorsRequest;
import com.acme.sponsorships.v1.ExportSponsorsResponse;
import com.acme.sponsorships.v1.SponsorQueryServiceGrpc;
import com.acme.treasury.v1.ImportLiquidityForecastsRequest;
import com.acme.treasury.v1.ImportLiquidityForecastsResponse;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class EInvoiceReportingApplicationTest {
    @Test
    void generateEInvoiceReportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var application = new EInvoiceReportingApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                DeliveryQueryServiceGrpc.newBlockingStub(channel),
                CryptoKeyServiceGrpc.newBlockingStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                repository,
                SponsorQueryServiceGrpc.newBlockingStub(channel),
                LiquidityForecastImportServiceGrpc.newStub(channel));
        var request = ProbeSupport.sample(GenerateEInvoiceReportRequest.getDefaultInstance());
        var response = application.generateEInvoiceReport(request);
        var sent0 = channel.request("acme.ledger.v1.LedgerAccountValidationService/ExplainLedgerAccountDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "ledger_account", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "locale"),
                ProbeSupport.value(sent0, "details", "locale"));
        var sent1 = channel.request("acme.keymanagement.v1.CryptoKeyService/CreateCryptoKey");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/ExplainLedgerAccountDecision"), "ledger_account", "bank", "bic"),
                ProbeSupport.value(sent1, "crypto_key", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/ExplainLedgerAccountDecision"), "ledger_account", "audit", "created_by"),
                ProbeSupport.value(sent1, "crypto_key", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/ExplainLedgerAccountDecision"), "ledger_account", "bank", "iban"),
                ProbeSupport.value(sent1, "crypto_key", "bank", "iban"));
        var sent2 = channel.request("acme.deliveries.v1.DeliveryQueryService/BrowseDeliveries");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/ExplainLedgerAccountDecision"), "ledger_account", "labels"),
                ProbeSupport.value(sent2, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.keymanagement.v1.CryptoKeyService/CreateCryptoKey"), "crypto_key", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/ExplainLedgerAccountDecision"), "ledger_account", "ledger_account_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/ExplainLedgerAccountDecision"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.keymanagement.v1.CryptoKeyService/CreateCryptoKey"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.deliveries.v1.DeliveryQueryService/BrowseDeliveries"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }

    @Test
    void streamEInvoiceMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var application = new EInvoiceReportingApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                DeliveryQueryServiceGrpc.newBlockingStub(channel),
                CryptoKeyServiceGrpc.newBlockingStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                repository,
                SponsorQueryServiceGrpc.newBlockingStub(channel),
                LiquidityForecastImportServiceGrpc.newStub(channel));
        var request = ProbeSupport.sample(StreamEInvoiceMetricsRequest.getDefaultInstance());
        var response = application.streamEInvoiceMetrics(request);
        var sent0 = channel.request("acme.sponsorships.v1.SponsorQueryService/ExportSponsors");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "query"),
                ProbeSupport.value(sent0, "filter", "query"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "e_invoice", "billing_address", "region"));
        var endpoint = new EInvoiceReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamEInvoiceMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamEInvoiceMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamEInvoiceMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
