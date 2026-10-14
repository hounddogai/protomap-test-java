package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.ExportAuditTrailsRequest;
import com.acme.audit.v1.ExportAuditTrailsResponse;
import com.acme.audit.v1.SuggestAuditTrailsRequest;
import com.acme.audit.v1.SuggestAuditTrailsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.RecalculateCoveragesRequest;
import com.acme.insurance.v1.RecalculateCoveragesResponse;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesRequest;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesResponse;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityResponse;
import com.acme.invoicing.v1.CloneInvoiceBatchRequest;
import com.acme.invoicing.v1.CloneInvoiceBatchResponse;
import com.acme.invoicing.v1.CompareEInvoiceOptionsRequest;
import com.acme.invoicing.v1.CompareEInvoiceOptionsResponse;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsResponse;
import com.acme.invoicing.v1.CountInvoiceBatchesRequest;
import com.acme.invoicing.v1.CountInvoiceBatchesResponse;
import com.acme.invoicing.v1.CreateInvoiceBatchRequest;
import com.acme.invoicing.v1.CreateInvoiceBatchResponse;
import com.acme.invoicing.v1.DeleteInvoiceBatchRequest;
import com.acme.invoicing.v1.DeleteInvoiceBatchResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceCalculationServiceGrpc;
import com.acme.invoicing.v1.EInvoiceCardSettlement;
import com.acme.invoicing.v1.EInvoiceLine;
import com.acme.invoicing.v1.EInvoiceLineTax;
import com.acme.invoicing.v1.EInvoiceValidationServiceGrpc;
import com.acme.invoicing.v1.ExplainInvoiceTemplateDecisionRequest;
import com.acme.invoicing.v1.ExplainInvoiceTemplateDecisionResponse;
import com.acme.invoicing.v1.GetInvoiceBatchRequest;
import com.acme.invoicing.v1.GetInvoiceBatchResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchBankSettlement;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchCardSettlement;
import com.acme.invoicing.v1.InvoiceBatchQueryServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateAttachment;
import com.acme.invoicing.v1.InvoiceTemplateBankSettlement;
import com.acme.invoicing.v1.InvoiceTemplateCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateQueryServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.LintEInvoicesRequest;
import com.acme.invoicing.v1.LintEInvoicesResponse;
import com.acme.invoicing.v1.LintInvoiceBatchesRequest;
import com.acme.invoicing.v1.LintInvoiceBatchesResponse;
import com.acme.invoicing.v1.ListInvoiceBatchesRequest;
import com.acme.invoicing.v1.ListInvoiceBatchesResponse;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateRequest;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateResponse;
import com.acme.invoicing.v1.QuoteInvoiceBatchRequest;
import com.acme.invoicing.v1.QuoteInvoiceBatchResponse;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesResponse;
import com.acme.invoicing.v1.ScoreInvoiceTemplateRequest;
import com.acme.invoicing.v1.ScoreInvoiceTemplateResponse;
import com.acme.invoicing.v1.UpdateInvoiceBatchRequest;
import com.acme.invoicing.v1.UpdateInvoiceBatchResponse;
import com.acme.invoicing.v1.ValidateInvoiceBatchRequest;
import com.acme.invoicing.v1.ValidateInvoiceBatchResponse;
import com.acme.invoicing.v1.ValidateInvoiceTemplateRequest;
import com.acme.invoicing.v1.ValidateInvoiceTemplateResponse;
import com.acme.invoicing.v1.WatchInvoiceBatchesRequest;
import com.acme.invoicing.v1.WatchInvoiceBatchesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.SearchMetricSeriesesRequest;
import com.acme.metrics.v1.SearchMetricSeriesesResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.ValidateNotificationRequest;
import com.acme.notifications.v1.ValidateNotificationResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.RecalculateBankTransfersRequest;
import com.acme.treasury.v1.RecalculateBankTransfersResponse;
import com.acme.venues.v1.PreviewVenueRequest;
import com.acme.venues.v1.PreviewVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class InvoiceBatchApplicationTest {
    @Test
    void updateInvoiceBatchPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var listener = new InvoiceBatchApplicationEvents(AuditTrailQueryServiceGrpc.newStub(channel), BankTransferCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof InvoiceBatchApplication.DeleteInvoiceBatchStage2Requested command) {
                listener.on(command);
            }
            else if (event instanceof InvoiceBatchApplication.WatchInvoiceBatchesStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new InvoiceBatchApplication(
                AuditTrailQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                events,
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                EInvoiceCalculationServiceGrpc.newBlockingStub(channel),
                EInvoiceValidationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchQueryServiceGrpc.newBlockingStub(channel),
                InvoiceBatchValidationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateQueryServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateValidationServiceGrpc.newBlockingStub(channel),
                new InvoiceBatchRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newFutureStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(UpdateInvoiceBatchRequest.getDefaultInstance());
        var response = application.updateInvoiceBatch(request);
        var sent0 = channel.request("acme.metrics.v1.MetricSeriesService/SearchMetricSerieses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.compliance.v1.ComplianceCaseCalculationService/ExplainComplianceCaseCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "audit", "updated_at"),
                ProbeSupport.value(sent1, "compliance_case", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "compliance_case", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "audit", "updated_by"),
                ProbeSupport.value(sent1, "compliance_case", "audit", "updated_by"));
        var sent2 = channel.request("acme.invoicing.v1.InvoiceBatchCalculationService/QuoteInvoiceBatch");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "invoice_batch", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "audit", "created_by"),
                ProbeSupport.value(sent2, "invoice_batch", "audit", "created_by"));
        var sent3 = channel.request("acme.invoicing.v1.InvoiceTemplateCalculationService/CompareInvoiceTemplateOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "billing_address", "recipient"),
                ProbeSupport.value(sent3, "details", "destination", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/QuoteInvoiceBatch"), "invoice_batch", "audit", "created_by"),
                ProbeSupport.value(sent3, "invoice_template", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/QuoteInvoiceBatch"), "invoice_batch", "audit", "updated_by"),
                ProbeSupport.value(sent3, "invoice_template", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesService/SearchMetricSerieses"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/ExplainComplianceCaseCalculation"), "compliance_case", "audit", "etag"),
                ProbeSupport.value(response, "invoice_batch", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchCalculationService/QuoteInvoiceBatch"), "invoice_batch", "audit", "created_by"),
                ProbeSupport.value(response, "invoice_batch", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateCalculationService/CompareInvoiceTemplateOptions"), "invoice_template", "audit", "updated_at"),
                ProbeSupport.value(response, "invoice_batch", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "issued_at"),
                ProbeSupport.value(response, "invoice_batch", "issued_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_batch", "counterparty"),
                ProbeSupport.value(response, "invoice_batch", "counterparty"));
    }

    @Test
    void watchInvoiceBatchesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var listener = new InvoiceBatchApplicationEvents(AuditTrailQueryServiceGrpc.newStub(channel), BankTransferCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof InvoiceBatchApplication.DeleteInvoiceBatchStage2Requested command) {
                listener.on(command);
            }
            else if (event instanceof InvoiceBatchApplication.WatchInvoiceBatchesStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new InvoiceBatchApplication(
                AuditTrailQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                events,
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                EInvoiceCalculationServiceGrpc.newBlockingStub(channel),
                EInvoiceValidationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchQueryServiceGrpc.newBlockingStub(channel),
                InvoiceBatchValidationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateQueryServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateValidationServiceGrpc.newBlockingStub(channel),
                new InvoiceBatchRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newFutureStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(WatchInvoiceBatchesRequest.getDefaultInstance());
        var response = application.watchInvoiceBatches(request);
        var sent0 = channel.request("acme.invoicing.v1.InvoiceTemplateValidationService/ScoreInvoiceTemplate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "details", "destination", "region"));
        var sent1 = channel.request("acme.treasury.v1.BankTransferCalculationService/RecalculateBankTransfers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/ScoreInvoiceTemplate"), "invoice_template", "billing_address", "lines"),
                ProbeSupport.value(sent1, "details", "destination", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/ScoreInvoiceTemplate"), "invoice_template", "billing_address", "recipient"),
                ProbeSupport.value(sent1, "bank_transfer", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/ScoreInvoiceTemplate"), "invoice_template", "audit", "updated_at"),
                ProbeSupport.value(sent1, "bank_transfer", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/ScoreInvoiceTemplate"), "invoice_template", "billing_address", "locality"),
                ProbeSupport.value(sent1, "details", "destination", "locality"));
        var sent2 = channel.request("acme.invoicing.v1.InvoiceBatchValidationService/CheckInvoiceBatchEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/RecalculateBankTransfers"), "bank_transfer", "audit", "updated_at"),
                ProbeSupport.value(sent2, "invoice_batch", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/ScoreInvoiceTemplate"), "invoice_template", "bank", "bic"),
                ProbeSupport.value(sent2, "invoice_batch", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceTemplateValidationService/ScoreInvoiceTemplate"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/RecalculateBankTransfers"), "bank_transfer", "counterparty"),
                ProbeSupport.value(response, "invoice_batch", "counterparty"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/CheckInvoiceBatchEligibility"), "invoice_batch", "audit", "created_by"),
                ProbeSupport.value(response, "invoice_batch", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "invoice_batch", "billing_address", "region"));
        var endpoint = new InvoiceBatchEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<WatchInvoiceBatchesResponse> observer = new StreamObserver<>() {
            public void onNext(WatchInvoiceBatchesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.watchInvoiceBatches(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void batchCreateInvoiceBatchesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var listener = new InvoiceBatchApplicationEvents(AuditTrailQueryServiceGrpc.newStub(channel), BankTransferCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof InvoiceBatchApplication.DeleteInvoiceBatchStage2Requested command) {
                listener.on(command);
            }
            else if (event instanceof InvoiceBatchApplication.WatchInvoiceBatchesStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new InvoiceBatchApplication(
                AuditTrailQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                events,
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                EInvoiceCalculationServiceGrpc.newBlockingStub(channel),
                EInvoiceValidationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceBatchQueryServiceGrpc.newBlockingStub(channel),
                InvoiceBatchValidationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateQueryServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateValidationServiceGrpc.newBlockingStub(channel),
                new InvoiceBatchRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newFutureStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(BatchCreateInvoiceBatchesRequest.getDefaultInstance());
        var response = application.batchCreateInvoiceBatches(request);
        var sent0 = channel.request("acme.invoicing.v1.InvoiceBatchQueryService/CountInvoiceBatches");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "issued_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "counterparty", "customer_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "metadata"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.invoicing.v1.InvoiceBatchValidationService/LintInvoiceBatches");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "card", "network"),
                ProbeSupport.value(sent1, "invoice_batch", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "billing_address", "recipient"),
                ProbeSupport.value(sent1, "invoice_batch", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "created_by"),
                ProbeSupport.value(sent1, "invoice_batch", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "header", "submitted_by", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        var sent2 = channel.request("acme.invoicing.v1.EInvoiceValidationService/LintEInvoices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "lines", "tax", "amount"),
                ProbeSupport.value(sent2, "e_invoice", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "lines", "tax", "jurisdiction"),
                ProbeSupport.value(sent2, "e_invoice", "lines", "tax", "jurisdiction"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "lines", "tax", "rate"),
                ProbeSupport.value(sent2, "e_invoice", "lines", "tax", "rate"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "lines", "description"),
                ProbeSupport.value(sent2, "e_invoice", "lines", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchQueryService/CountInvoiceBatches"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchQueryService/CountInvoiceBatches"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/LintInvoiceBatches"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/LintInvoiceBatches"), "invoice_batch", "invoice_batch_id"),
                ProbeSupport.value(response, "rejections", "invoice_batch_id"));
        var endpoint = new InvoiceBatchEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreateInvoiceBatchesResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreateInvoiceBatchesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreateInvoiceBatches(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new InvoiceBatchApplicationEvents(AuditTrailQueryServiceGrpc.newStub(channel), BankTransferCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<SuggestAuditTrailsResponse>();
        listener.on(new InvoiceBatchApplication.DeleteInvoiceBatchStage2Requested(ProbeSupport.sample(SuggestAuditTrailsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<RecalculateBankTransfersResponse>();
        listener.on(new InvoiceBatchApplication.WatchInvoiceBatchesStage1Requested(ProbeSupport.sample(RecalculateBankTransfersRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
