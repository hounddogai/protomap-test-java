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
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
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
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about e invoices. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class EInvoiceReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(EInvoiceReportingApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final DeliveryQueryServiceGrpc.DeliveryQueryServiceBlockingStub deliveriesDeliveryQueryServiceBlocking;

    private final CryptoKeyServiceGrpc.CryptoKeyServiceBlockingStub keymanagementCryptoKeyServiceBlocking;

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final InvoicingRepository repository;

    private final SponsorQueryServiceGrpc.SponsorQueryServiceBlockingStub sponsorshipsSponsorQueryServiceBlocking;

    private final LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceStub treasuryLiquidityForecastImportServiceAsync;

    public EInvoiceReportingApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            DeliveryQueryServiceGrpc.DeliveryQueryServiceBlockingStub deliveriesDeliveryQueryServiceBlocking,
            CryptoKeyServiceGrpc.CryptoKeyServiceBlockingStub keymanagementCryptoKeyServiceBlocking,
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            InvoicingRepository repository,
            SponsorQueryServiceGrpc.SponsorQueryServiceBlockingStub sponsorshipsSponsorQueryServiceBlocking,
            LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceStub treasuryLiquidityForecastImportServiceAsync) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.budgetingVarianceSyncServiceAsync = budgetingVarianceSyncServiceAsync;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.deliveriesDeliveryQueryServiceBlocking = deliveriesDeliveryQueryServiceBlocking;
        this.keymanagementCryptoKeyServiceBlocking = keymanagementCryptoKeyServiceBlocking;
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.repository = repository;
        this.sponsorshipsSponsorQueryServiceBlocking = sponsorshipsSponsorQueryServiceBlocking;
        this.treasuryLiquidityForecastImportServiceAsync = treasuryLiquidityForecastImportServiceAsync;
    }

    /**
     * Generates an e invoice.
     */
    public GenerateEInvoiceReportResponse generateEInvoiceReport(GenerateEInvoiceReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateEInvoiceReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "GenerateEInvoiceReport");

        var outbound0 = ExplainLedgerAccountDecisionRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ExplainLedgerAccountDecisionRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLedgerAccountDecision(outbound0);

        var outbound1 = CreateCryptoKeyRequest.newBuilder()
                .setContext(request.getContext())
                .setCryptoKey(CryptoKey.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getLedgerAccount().getAudit().getCreatedBy())
                                .build())
                        .setBank(CryptoKeyBankFunding.newBuilder()
                                .setBic(switch (reply0.getLedgerAccount().getFundingCase()) {
                                    case BANK -> reply0.getLedgerAccount().getBank().getBic();
                                    default -> "";
                                })
                                .setIban(switch (reply0.getLedgerAccount().getFundingCase()) {
                                    case BANK -> reply0.getLedgerAccount().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = keymanagementCryptoKeyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createCryptoKey(outbound1);

        var outbound2 = JsonCodec.roundTrip(BrowseDeliveriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseDeliveriesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getCryptoKey().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getLedgerAccount().getLabelsMap())
                        .setOwnerId(reply0.getLedgerAccount().getLedgerAccountId())
                        .build())
                .build(), BrowseDeliveriesRequest.class);
        var reply2 = deliveriesDeliveryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseDeliveries(outbound2);

        var outbound3 = ImportLiquidityForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(ImportLiquidityForecastsRequest.StreamHeader.newBuilder()
                        .setFormat(request.getDetails().getFormat())
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(ImportLiquidityForecastsResponse.getDefaultInstance());
        var input3 = treasuryLiquidityForecastImportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).importLiquidityForecasts(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamVarianceChangesRequest.newBuilder()
                .setChange(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamVarianceChangesResponse.getDefaultInstance());
        var input4 = budgetingVarianceSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamVarianceChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateEInvoiceReport", request.getEInvoiceId());
        return GenerateEInvoiceReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an e invoice.
     */
    public FetchEInvoiceReportResponse fetchEInvoiceReport(FetchEInvoiceReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchEInvoiceReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "FetchEInvoiceReport");
        return FetchEInvoiceReportResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .build();
    }

    /**
     * Streams the changes of e invoices as they happen.
     */
    public StreamEInvoiceMetricsResponse streamEInvoiceMetrics(StreamEInvoiceMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamEInvoiceMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getContext().getTraceId(), "StreamEInvoiceMetrics");

        var outbound0 = ExportSponsorsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSponsorsRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setQuery(request.getFilter().getQuery())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var stream0 = sponsorshipsSponsorQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSponsors(outbound0);
        var reply0 = ExportSponsorsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFilter().getOwnerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamEInvoiceMetrics", request.getFilter().getOwnerId());
        return StreamEInvoiceMetricsResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists e invoices a page at a time.
     */
    public ListEInvoiceReportsResponse listEInvoiceReports(ListEInvoiceReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListEInvoiceReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getContext().getTraceId(), "ListEInvoiceReports");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListEInvoiceReports", request.getPage().getPageToken());
        return ListEInvoiceReportsResponse.getDefaultInstance();
    }

    /**
     * Shares an e invoice.
     */
    public ShareEInvoiceReportResponse shareEInvoiceReport(ShareEInvoiceReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareEInvoiceReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "ShareEInvoiceReport");

        var outbound0 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .setName(request.getDetails().getRecipient().getName())
                                .setPhone(request.getDetails().getRecipient().getPhone())
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input0 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareEInvoiceReport", request.getDetails().getTemplateId());
        return ShareEInvoiceReportResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .build();
    }

    /**
     * Counts e invoices by status.
     */
    public CountEInvoiceTotalsResponse countEInvoiceTotals(CountEInvoiceTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountEInvoiceTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getContext().getTraceId(), "CountEInvoiceTotals");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountEInvoiceTotals", request.getPage().getPageToken());
        return CountEInvoiceTotalsResponse.getDefaultInstance();
    }

    /**
     * Deletes an e invoice.
     */
    public DeleteEInvoiceReportResponse deleteEInvoiceReport(DeleteEInvoiceReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteEInvoiceReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "DeleteEInvoiceReport");

        var outbound0 = (CompareConsentRecordOptionsRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setCorrelationId(request.getEInvoiceId())
                        .setDetail(Any.pack(request.getContext()))
                        .setOccurredAt(request.getContext().getDeadline())
                        .build())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build()).toBuilder().build();
        var reply0 = customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareConsentRecordOptions(outbound0);

        var outbound1 = PushRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushRetentionPolicyRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getConsentRecord().getSubject().getDisplayName())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushRetentionPolicy(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getEtag()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteEInvoiceReport", request.getEtag());
        return DeleteEInvoiceReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getConsentRecord().getCorrelationId())
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
