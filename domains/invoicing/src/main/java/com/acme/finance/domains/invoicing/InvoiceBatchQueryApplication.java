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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about invoice batches, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class InvoiceBatchQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceBatchQueryApplication.class);

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking;

    private final InvoiceTemplateCalculationServiceGrpc.InvoiceTemplateCalculationServiceBlockingStub invoicingInvoiceTemplateCalculationServiceBlocking;

    private final InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking;

    private final InvoiceBatchQueryRequestMapper mapper;

    private final InvoicingRepository repository;

    private final RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceBlockingStub riskRiskRegisterQueryServiceBlocking;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    public InvoiceBatchQueryApplication(
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking,
            InvoiceTemplateCalculationServiceGrpc.InvoiceTemplateCalculationServiceBlockingStub invoicingInvoiceTemplateCalculationServiceBlocking,
            InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking,
            InvoiceBatchQueryRequestMapper mapper,
            InvoicingRepository repository,
            RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceBlockingStub riskRiskRegisterQueryServiceBlocking,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking) {
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.budgetingBudgetLineValidationServiceBlocking = budgetingBudgetLineValidationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.invoicingInvoiceBatchValidationServiceBlocking = invoicingInvoiceBatchValidationServiceBlocking;
        this.invoicingInvoiceTemplateCalculationServiceBlocking = invoicingInvoiceTemplateCalculationServiceBlocking;
        this.invoicingInvoiceTemplateValidationServiceBlocking = invoicingInvoiceTemplateValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskRegisterQueryServiceBlocking = riskRiskRegisterQueryServiceBlocking;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
    }

    public record ExportInvoiceBatchesStage0Requested(ValidateEInvoiceRequest request, CompletableFuture<ValidateEInvoiceResponse> completion, io.grpc.Context context) {}

    /**
     * Returns an invoice batch.
     */
    public LookupInvoiceBatchResponse lookupInvoiceBatch(LookupInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getInvoiceBatchId(), "LookupInvoiceBatch");

        var outbound0 = RetryAuditTrailExportRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RetryAuditTrailExportRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllParameters(request.getContext().getBaggageMap())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryAuditTrailExport(value));
        return LookupInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Lists invoice batches a page at a time.
     */
    public BrowseInvoiceBatchesResponse browseInvoiceBatches(BrowseInvoiceBatchesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseInvoiceBatches")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getContext().getTraceId(), "BrowseInvoiceBatches");

        var outbound0 = mapper.browseInvoiceBatchesStage0(request);
        var reply0 = budgetingBudgetLineValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeBudgetLine(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseInvoiceBatches", request.getPage().getPageToken());
        return BrowseInvoiceBatchesResponse.newBuilder()
                .addInvoiceBatches(InvoiceBatch.newBuilder()
                        .setNotes(reply0.getBudgetLine().getNotes())
                        .build())
                .build();
    }

    /**
     * Lists invoice batches a page at a time.
     */
    public FindInvoiceBatchesResponse findInvoiceBatches(FindInvoiceBatchesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindInvoiceBatches")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getContext().getTraceId(), "FindInvoiceBatches");

        var outbound0 = NormalizeInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeInvoiceBatchRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeInvoiceBatch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindInvoiceBatches", request.getPage().getPageToken());
        return FindInvoiceBatchesResponse.newBuilder()
                .addInvoiceBatches(InvoiceBatch.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getInvoiceBatch().getCounterparty().getEmail())
                                .build())
                        .setNotes(reply0.getInvoiceBatch().getNotes())
                        .build())
                .build();
    }

    /**
     * Counts invoice batches by status.
     */
    public CountInvoiceBatchesResponse countInvoiceBatches(CountInvoiceBatchesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountInvoiceBatches")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getContext().getTraceId(), "CountInvoiceBatches");

        var outbound0 = CheckInvoiceTemplateEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckInvoiceTemplateEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkInvoiceTemplateEligibility(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountInvoiceBatches", request.getPage().getPageToken());
        return CountInvoiceBatchesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several invoice batches at once.
     */
    public ResolveInvoiceBatchesResponse resolveInvoiceBatches(ResolveInvoiceBatchesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveInvoiceBatches")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getContext().getTraceId(), "ResolveInvoiceBatches");

        var outbound0 = ExplainInvoiceTemplateDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainInvoiceTemplateDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var reply0 = invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainInvoiceTemplateDecision(outbound0);
        return ResolveInvoiceBatchesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Streams every matching invoice batch.
     */
    public ExportInvoiceBatchesResponse exportInvoiceBatches(ExportInvoiceBatchesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportInvoiceBatches")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getContext().getTraceId(), "ExportInvoiceBatches");

        var outbound0 = ValidateEInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateEInvoiceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ValidateEInvoiceResponse>();
        events.publishEvent(new ExportInvoiceBatchesStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = mapper.exportInvoiceBatchesStage1(request, reply0);
        var reply1 = riskRiskRegisterQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestRiskRegisters(outbound1);

        var outbound2 = ValidateSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getEInvoice().getBillingAddress().getCountryCode())
                                .setLocality(reply0.getEInvoice().getBillingAddress().getLocality())
                                .setRegion(reply0.getEInvoice().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSearchIndex(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = QuoteClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply2.getSearchIndex().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteClaimRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getEInvoice().getBillingAddress().getCountryCode())
                                .setLocality(reply0.getEInvoice().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.insurance.v1.ClaimCalculationService");
        var serviceName3 = "acme.insurance.v1" + "." + "ClaimCalculationService";
        var methodName3 = "Quo" + "teClaim";
        var descriptor3 = MethodDescriptor.<QuoteClaimRequest, QuoteClaimResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(QuoteClaimRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(QuoteClaimResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportInvoiceBatches", request.getFilter().getQuery());
        return ExportInvoiceBatchesResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(reply0.getEInvoice().getBillingAddress())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply3.getClaim().getCustomer().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists invoice batches a page at a time.
     */
    public SuggestInvoiceBatchesResponse suggestInvoiceBatches(SuggestInvoiceBatchesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestInvoiceBatches")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getContext().getTraceId(), "SuggestInvoiceBatches");

        var outbound0 = SimulateInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .addLines(InvoiceTemplateLine.newBuilder()
                                .putAllAttributes(request.getFilter().getLabelsMap())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.invoicing.v1.InvoiceTemplateCalculationService/SimulateInvoiceTemplate", outbound0);
        var reply0 = invoicingInvoiceTemplateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateInvoiceTemplate(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestInvoiceBatches", request.getPage().getPageToken());
        return SuggestInvoiceBatchesResponse.newBuilder()
                .addInvoiceBatches(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getInvoiceTemplate().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns an invoice batch.
     */
    public PeekInvoiceBatchResponse peekInvoiceBatch(PeekInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getInvoiceBatchId(), "PeekInvoiceBatch");
        return PeekInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .build())
                .build();
    }
}
