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
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.CalculateInvoiceBatchRequest;
import com.acme.invoicing.v1.CalculateInvoiceBatchResponse;
import com.acme.invoicing.v1.CalculateInvoiceTemplateRequest;
import com.acme.invoicing.v1.CalculateInvoiceTemplateResponse;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsResponse;
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
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.PayStub;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of an invoice template, such as totals, taxes, and estimates. Coordinates validation,
 * durable audit writes, and downstream operations.
 */
@Service
public class InvoiceTemplateCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceTemplateCalculationApplication.class);

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final ContractQueryServiceGrpc.ContractQueryServiceBlockingStub contractsContractQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking;

    private final InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking;

    private final InvoicingRepository repository;

    private final Executor rpcExecutor;

    public InvoiceTemplateCalculationApplication(
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            ContractQueryServiceGrpc.ContractQueryServiceBlockingStub contractsContractQueryServiceBlocking,
            ApplicationEventPublisher events,
            FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking,
            InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking,
            InvoicingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.contractsContractQueryServiceBlocking = contractsContractQueryServiceBlocking;
        this.events = events;
        this.featureflagsFeatureFlagQueryServiceBlocking = featureflagsFeatureFlagQueryServiceBlocking;
        this.invoicingInvoiceBatchCalculationServiceBlocking = invoicingInvoiceBatchCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record EstimateInvoiceTemplateStage1Requested(EstimatePayStubRequest request, CompletableFuture<EstimatePayStubResponse> completion, io.grpc.Context context) {}

    public record ExplainInvoiceTemplateCalculationStage0Requested(BrowseJournalEntriesRequest request, CompletableFuture<BrowseJournalEntriesResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates an invoice template.
     */
    public CalculateInvoiceTemplateResponse calculateInvoiceTemplate(CalculateInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = JsonCodec.roundTrip(BrowseFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseFeatureFlagsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoiceTemplate().getAudit().getUpdatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getInvoiceTemplate().getCounterparty().getCustomerId())
                        .setRegion(request.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build(), BrowseFeatureFlagsRequest.class);
        var reply0 = featureflagsFeatureFlagQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseFeatureFlags(outbound0);

        var results1 = new ArrayList<CalculateInvoiceBatchResponse>();
        for (var element : request.getInvoiceTemplate().getLinesList()) {
            CalculateInvoiceBatchRequest itemRequest = CalculateInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .addLines(InvoiceBatchLine.newBuilder()
                                .setTax(InvoiceBatchLineTax.newBuilder()
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build();
            results1.add(invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateInvoiceBatch(itemRequest));
        }
        var reply1 = results1.isEmpty() ? CalculateInvoiceBatchResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = (AnnotateAuditEventVersionRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setAuditEvent(AuditEvent.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getInvoiceTemplate().getCounterparty().getDisplayName())
                                .setNumber(request.getInvoiceTemplate().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(AnnotateAuditEventVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build()).toBuilder().build();
        var reply2 = auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateAuditEventVersion(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateInvoiceTemplate", request.getInvoiceTemplate().getInvoiceTemplateId());
        return CalculateInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .putAllMetadata(request.getInvoiceTemplate().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Estimates an invoice template.
     */
    public EstimateInvoiceTemplateResponse estimateInvoiceTemplate(EstimateInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = PreviewStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(request.getInvoiceTemplate().getBillingAddress().getPostalCode())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getInvoiceTemplate().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewStandingOrder(outbound0);

        var outbound1 = EstimatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getStandingOrder().getLocation().getPostalCode())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<EstimatePayStubResponse>();
        events.publishEvent(new EstimateInvoiceTemplateStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateInvoiceTemplate", request.getInvoiceTemplate().getInvoiceTemplateId());
        return EstimateInvoiceTemplateResponse.newBuilder()
                .setBreakdown(EstimateInvoiceTemplateResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getPayStub().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getStandingOrder().getOrganizer().getEmail())
                                .build())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .setNotes(reply1.getPayStub().getNotes())
                        .build())
                .build();
    }

    /**
     * Quotes an invoice template.
     */
    public QuoteInvoiceTemplateResponse quoteInvoiceTemplate(QuoteInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = FindContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoiceTemplate().getDueAt())
                        .putAllLabels(request.getInvoiceTemplate().getMetadataMap())
                        .setOwnerId(request.getInvoiceTemplate().getCounterparty().getCustomerId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> contractsContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findContracts(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteInvoiceTemplate", request.getInvoiceTemplate().getInvoiceTemplateId());
        return QuoteInvoiceTemplateResponse.newBuilder()
                .setBreakdown(QuoteInvoiceTemplateResponse.Breakdown.newBuilder()
                        .setTotal(request.getInvoiceTemplate().getTotal())
                        .build())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Compares an invoice template.
     */
    public CompareInvoiceTemplateOptionsResponse compareInvoiceTemplateOptions(CompareInvoiceTemplateOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareInvoiceTemplateOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var futures0 = request.getInvoiceTemplate().getAttachmentsList().stream()
                .map(element -> SimulateInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .addAttachments(InvoiceBatchAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setFileName(element.getFileName())
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateInvoiceBatch(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? SimulateInvoiceBatchResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareInvoiceTemplateOptions", request.getInvoiceTemplate().getInvoiceTemplateId());
        return CompareInvoiceTemplateOptionsResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .setIssuedAt(request.getInvoiceTemplate().getIssuedAt())
                        .build())
                .build();
    }

    /**
     * Simulates an invoice template.
     */
    public SimulateInvoiceTemplateResponse simulateInvoiceTemplate(SimulateInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = BrowseFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseFeatureFlagsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoiceTemplate().getDueAt())
                        .setOwnerId(request.getInvoiceTemplate().getCounterparty().getCustomerId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> featureflagsFeatureFlagQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseFeatureFlags(outbound0), rpcExecutor);

        var outbound1 = RecalculateInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getInvoiceTemplate().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getInvoiceTemplate().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getInvoiceTemplate().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateInvoiceBatches(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateInvoiceTemplate", request.getInvoiceTemplate().getInvoiceTemplateId());
        return SimulateInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setBillingAddress(request.getInvoiceTemplate().getBillingAddress())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains an invoice template.
     */
    public ExplainInvoiceTemplateCalculationResponse explainInvoiceTemplateCalculation(ExplainInvoiceTemplateCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainInvoiceTemplateCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = BrowseJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseJournalEntriesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoiceTemplate().getAudit().getCreatedAt())
                        .setOwnerId(request.getInvoiceTemplate().getCounterparty().getCustomerId())
                        .setRegion(request.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion0 = new CompletableFuture<BrowseJournalEntriesResponse>();
        events.publishEvent(new ExplainInvoiceTemplateCalculationStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainInvoiceTemplateCalculation", request.getInvoiceTemplate().getInvoiceTemplateId());
        return ExplainInvoiceTemplateCalculationResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setDueAt(request.getInvoiceTemplate().getDueAt())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an invoice template.
     */
    public GetInvoiceTemplateRatesResponse getInvoiceTemplateRates(GetInvoiceTemplateRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetInvoiceTemplateRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getInvoiceTemplateId(), "GetInvoiceTemplateRates");
        return GetInvoiceTemplateRatesResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .build();
    }
}
