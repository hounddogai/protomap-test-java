package com.acme.finance.domains.billing;

import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.ListAuditEventExportsRequest;
import com.acme.audit.v1.ListAuditEventExportsResponse;
import com.acme.audit.v1.ScheduleAuditEventExportRequest;
import com.acme.audit.v1.ScheduleAuditEventExportResponse;
import com.acme.audit.v1.ShareAuditEventExportRequest;
import com.acme.audit.v1.ShareAuditEventExportResponse;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.billing.v1.CalculateInvoiceRequest;
import com.acme.billing.v1.CalculateInvoiceResponse;
import com.acme.billing.v1.CompareInvoiceOptionsRequest;
import com.acme.billing.v1.CompareInvoiceOptionsResponse;
import com.acme.billing.v1.EstimateInvoiceRequest;
import com.acme.billing.v1.EstimateInvoiceResponse;
import com.acme.billing.v1.ExplainInvoiceCalculationRequest;
import com.acme.billing.v1.ExplainInvoiceCalculationResponse;
import com.acme.billing.v1.GetInvoiceRatesRequest;
import com.acme.billing.v1.GetInvoiceRatesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.RecalculateInvoicesRequest;
import com.acme.billing.v1.RecalculateInvoicesResponse;
import com.acme.billing.v1.SimulateInvoiceRequest;
import com.acme.billing.v1.SimulateInvoiceResponse;
import com.acme.boxoffice.v1.BoxOfficeSale;
import com.acme.boxoffice.v1.BoxOfficeSaleCalculationServiceGrpc;
import com.acme.boxoffice.v1.BoxOfficeSaleCardSettlement;
import com.acme.boxoffice.v1.BoxOfficeSaleLine;
import com.acme.boxoffice.v1.CalculateBoxOfficeSaleRequest;
import com.acme.boxoffice.v1.CalculateBoxOfficeSaleResponse;
import com.acme.boxoffice.v1.CashDrawerRef;
import com.acme.boxoffice.v1.RecalculateBoxOfficeSalesRequest;
import com.acme.boxoffice.v1.RecalculateBoxOfficeSalesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.NormalizePremiumRequest;
import com.acme.insurance.v1.NormalizePremiumResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PremiumLineTax;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchAttachment;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.ScoreInvoiceBatchRequest;
import com.acme.invoicing.v1.ScoreInvoiceBatchResponse;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountsRequest;
import com.acme.ledger.v1.ListLedgerAccountsResponse;
import com.acme.ledger.v1.SearchLedgerAccountsRequest;
import com.acme.ledger.v1.SearchLedgerAccountsResponse;
import com.acme.payroll.v1.BrowsePayRunsRequest;
import com.acme.payroll.v1.BrowsePayRunsResponse;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.SummarizePayRunsRequest;
import com.acme.payroll.v1.SummarizePayRunsResponse;
import com.acme.secrets.v1.ExportSecretsRequest;
import com.acme.secrets.v1.ExportSecretsResponse;
import com.acme.secrets.v1.SecretQueryServiceGrpc;
import com.acme.suppliers.v1.CountSuppliersRequest;
import com.acme.suppliers.v1.CountSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.treasury.v1.EstimateFxHedgeRequest;
import com.acme.treasury.v1.EstimateFxHedgeResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeLine;
import com.acme.treasury.v1.FxHedgeLineTax;
import com.acme.venues.v1.CountVenuesRequest;
import com.acme.venues.v1.CountVenuesResponse;
import com.acme.venues.v1.FindVenuesRequest;
import com.acme.venues.v1.FindVenuesResponse;
import com.acme.venues.v1.SuggestVenuesRequest;
import com.acme.venues.v1.SuggestVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of an invoice, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class InvoiceCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceCalculationApplication.class);

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final AuditEventExportServiceGrpc.AuditEventExportServiceFutureStub auditAuditEventExportServiceFuture;

    private final BoxOfficeSaleCalculationServiceGrpc.BoxOfficeSaleCalculationServiceBlockingStub boxofficeBoxOfficeSaleCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking;

    private final InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final InvoiceCalculationRequestMapper mapper;

    private final PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    private final SecretQueryServiceGrpc.SecretQueryServiceBlockingStub secretsSecretQueryServiceBlocking;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public InvoiceCalculationApplication(
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            AuditEventExportServiceGrpc.AuditEventExportServiceFutureStub auditAuditEventExportServiceFuture,
            BoxOfficeSaleCalculationServiceGrpc.BoxOfficeSaleCalculationServiceBlockingStub boxofficeBoxOfficeSaleCalculationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking,
            InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            InvoiceCalculationRequestMapper mapper,
            PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SecretQueryServiceGrpc.SecretQueryServiceBlockingStub secretsSecretQueryServiceBlocking,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.auditAuditEventExportServiceFuture = auditAuditEventExportServiceFuture;
        this.boxofficeBoxOfficeSaleCalculationServiceBlocking = boxofficeBoxOfficeSaleCalculationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.insurancePremiumValidationServiceBlocking = insurancePremiumValidationServiceBlocking;
        this.invoicingInvoiceBatchValidationServiceBlocking = invoicingInvoiceBatchValidationServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.mapper = mapper;
        this.payrollPayRunQueryServiceBlocking = payrollPayRunQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.secretsSecretQueryServiceBlocking = secretsSecretQueryServiceBlocking;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    public record ExplainInvoiceCalculationStage1Requested(ListAuditEventExportsRequest request, CompletableFuture<ListAuditEventExportsResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates an invoice.
     */
    public CalculateInvoiceResponse calculateInvoice(CalculateInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = ExportSecretsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSecretsRequest.Filter.newBuilder()
                        .setRegion(request.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = secretsSecretQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSecrets(outbound0);
        var reply0 = ExportSecretsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = RpcSupport.unpack(Any.pack(SummarizePayRunsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build()), SummarizePayRunsRequest.class);
        var reply1 = payrollPayRunQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizePayRuns(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateInvoice", request.getInvoice().getInvoiceId());
        return CalculateInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(request.getInvoice().getBillingAddress())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Estimates an invoice.
     */
    public EstimateInvoiceResponse estimateInvoice(EstimateInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        List<EstimateFxHedgeResponse> results0 = request.getInvoice().getLinesList().stream()
                .map(element -> EstimateFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .addLines(FxHedgeLine.newBuilder()
                                .setTax(FxHedgeLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFxHedge(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? EstimateFxHedgeResponse.getDefaultInstance()
                : results0.getLast();

        var results1 = new ArrayList<ScoreInvoiceBatchResponse>();
        for (var element : request.getInvoice().getAttachmentsList()) {
            ScoreInvoiceBatchRequest itemRequest = ScoreInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .addAttachments(InvoiceBatchAttachment.newBuilder()
                                .setContentType(element.getContentType())
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build();
            results1.add(invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreInvoiceBatch(itemRequest));
        }
        var reply1 = results1.isEmpty() ? ScoreInvoiceBatchResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateInvoice", request.getInvoice().getInvoiceId());
        return EstimateInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(request.getInvoice().getBillingAddress())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .build();
    }

    /**
     * Compares an invoice.
     */
    public CompareInvoiceOptionsResponse compareInvoiceOptions(CompareInvoiceOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareInvoiceOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = ShareAuditEventExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareAuditEventExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getInvoice().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ShareAuditEventExportRequest, ShareAuditEventExportResponse>() {
            @Override
            public ShareAuditEventExportResponse apply(ShareAuditEventExportRequest input) {
                return auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareAuditEventExport(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(ScoreInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreInvoiceBatchRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getInvoice().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getInvoice().getCounterparty().getEmail())
                                .build())
                        .build())
                .build()), ScoreInvoiceBatchRequest.class);
        var reply1 = invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreInvoiceBatch(outbound1);

        var outbound2 = mapper.compareInvoiceOptionsStage2(request);
        var reply2 = venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findVenues(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareInvoiceOptions", request.getInvoice().getInvoiceId());
        return CompareInvoiceOptionsResponse.newBuilder()
                .setBreakdown(CompareInvoiceOptionsResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getInvoiceBatch().getBillingAddress().getCountryCode())
                                .build())
                        .setDueAt(reply1.getInvoiceBatch().getDueAt())
                        .setInvoiceId(request.getInvoiceId())
                        .setNumber(reply0.getAuditEvent().getSubject().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Recalculates an invoice.
     */
    public RecalculateInvoicesResponse recalculateInvoices(RecalculateInvoicesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateInvoices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = SuggestVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoice().getIssuedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestVenues(outbound0), rpcExecutor);

        var outbound1 = SearchLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoice().getIssuedAt())
                        .setRegion(request.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchLedgerAccounts(outbound1), rpcExecutor);

        var outbound2 = BrowsePayRunsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowsePayRunsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoice().getAudit().getCreatedAt())
                        .setRegion(request.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> payrollPayRunQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browsePayRuns(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateInvoices", request.getInvoice().getInvoiceId());
        return RecalculateInvoicesResponse.newBuilder()
                .setBreakdown(RecalculateInvoicesResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setInvoice(Invoice.newBuilder()
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Simulates an invoice.
     */
    public SimulateInvoiceResponse simulateInvoice(SimulateInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = RpcSupport.unpack(Any.pack(CalculateBoxOfficeSaleRequest.newBuilder()
                .setBoxOfficeSale(BoxOfficeSale.newBuilder()
                        .setCard(BoxOfficeSaleCardSettlement.newBuilder()
                                .setHolderName(request.getInvoice().getCard().getHolderName())
                                .setLast4(request.getInvoice().getCard().getLast4())
                                .build())
                        .setCounterparty(CashDrawerRef.newBuilder()
                                .setDisplayName(request.getInvoice().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()), CalculateBoxOfficeSaleRequest.class);
        var reply0 = boxofficeBoxOfficeSaleCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateBoxOfficeSale(outbound0);

        var builder1 = CountVenuesRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFilter(CountVenuesRequest.Filter.newBuilder()
                .setCreatedAfter(request.getInvoice().getAudit().getUpdatedAt())
                .setOwnerId(reply0.getBoxOfficeSale().getCounterparty().getCashDrawerId())
                .setRegion(request.getInvoice().getBillingAddress().getRegion())
                .build());
        var outbound1 = builder1.build();
        var reply1 = venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countVenues(outbound1);

        var outbound2 = ScheduleAuditEventExportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending2 = auditAuditEventExportServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleAuditEventExport(outbound2);
        var reply2 = RpcSupport.await(pending2);

        var outbound3 = CountSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getBoxOfficeSale().getAudit().getCreatedAt())
                        .putAllLabels(request.getInvoice().getMetadataMap())
                        .setOwnerId(reply1.getResult().getCorrelationId())
                        .setRegion(reply0.getBoxOfficeSale().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.suppliers.v1.SupplierQueryService");
        var selected3 = SupplierQueryServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.countSuppliers(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateInvoice", request.getInvoice().getInvoiceId());
        return SimulateInvoiceResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getBoxOfficeSale().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getBoxOfficeSale().getBillingAddress().getLinesList())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .setStatus(request.getInvoice().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply3.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains an invoice.
     */
    public ExplainInvoiceCalculationResponse explainInvoiceCalculation(ExplainInvoiceCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainInvoiceCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoice(request.getContext().getTenantId(), request.getInvoice());

        var outbound0 = StreamAuditEventExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditEventExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoice().getIssuedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getInvoice().getCounterparty().getCustomerId())
                        .setRegion(request.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventExport(outbound0);
        var reply0 = StreamAuditEventExportResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = ListAuditEventExportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventExportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoice().getDueAt())
                        .putAllLabels(request.getInvoice().getMetadataMap())
                        .setOwnerId(request.getInvoice().getCounterparty().getCustomerId())
                        .setRegion(request.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ListAuditEventExportsResponse>();
        events.publishEvent(new ExplainInvoiceCalculationStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = mapper.explainInvoiceCalculationStage2(request);
        var reply2 = ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLedgerAccounts(outbound2);

        var futures3 = request.getInvoice().getLinesList().stream()
                .map(element -> NormalizePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .addLines(PremiumLine.newBuilder()
                                .setDescription(element.getDescription())
                                .setTax(PremiumLineTax.newBuilder()
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insurancePremiumValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePremium(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? NormalizePremiumResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoice().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainInvoiceCalculation", request.getInvoice().getNumber());
        return ExplainInvoiceCalculationResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setInvoiceId(request.getInvoiceId())
                        .setStatus(request.getInvoice().getStatus())
                        .build())
                .setOutcome(ExplainInvoiceCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns an invoice.
     */
    public GetInvoiceRatesResponse getInvoiceRates(GetInvoiceRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetInvoiceRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Invoice", request.getInvoiceId(), "GetInvoiceRates");

        var outbound0 = RecalculateBoxOfficeSalesRequest.newBuilder()
                .setBoxOfficeSale(BoxOfficeSale.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .addLines(BoxOfficeSaleLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command0 = new DispatchCommand<>("acme.boxoffice.v1.BoxOfficeSaleCalculationService/RecalculateBoxOfficeSales", outbound0);
        var reply0 = boxofficeBoxOfficeSaleCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateBoxOfficeSales(command0.payload());
        return GetInvoiceRatesResponse.newBuilder()
                .setInvoice(Invoice.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getBoxOfficeSale().getCounterparty().getDisplayName())
                                .build())
                        .setInvoiceId(request.getInvoiceId())
                        .build())
                .build();
    }
}
