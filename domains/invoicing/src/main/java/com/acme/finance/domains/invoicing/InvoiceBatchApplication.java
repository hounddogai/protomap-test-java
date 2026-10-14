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
import com.acme.treasury.v1.RecalculateBankTransfersRequest;
import com.acme.treasury.v1.RecalculateBankTransfersResponse;
import com.acme.venues.v1.PreviewVenueRequest;
import com.acme.venues.v1.PreviewVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Manages invoice batches: creates, reads, changes, and lists them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class InvoiceBatchApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceBatchApplication.class);

    private final AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceBlockingStub invoicingEInvoiceCalculationServiceBlocking;

    private final EInvoiceValidationServiceGrpc.EInvoiceValidationServiceBlockingStub invoicingEInvoiceValidationServiceBlocking;

    private final InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking;

    private final InvoiceBatchQueryServiceGrpc.InvoiceBatchQueryServiceBlockingStub invoicingInvoiceBatchQueryServiceBlocking;

    private final InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking;

    private final InvoiceTemplateCalculationServiceGrpc.InvoiceTemplateCalculationServiceBlockingStub invoicingInvoiceTemplateCalculationServiceBlocking;

    private final InvoiceTemplateQueryServiceGrpc.InvoiceTemplateQueryServiceBlockingStub invoicingInvoiceTemplateQueryServiceBlocking;

    private final InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking;

    private final InvoiceBatchRequestMapper mapper;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceFutureStub notificationsNotificationValidationServiceFuture;

    private final PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking;

    private final InvoicingRepository repository;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public InvoiceBatchApplication(
            AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            ApplicationEventPublisher events,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceBlockingStub invoicingEInvoiceCalculationServiceBlocking,
            EInvoiceValidationServiceGrpc.EInvoiceValidationServiceBlockingStub invoicingEInvoiceValidationServiceBlocking,
            InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking,
            InvoiceBatchQueryServiceGrpc.InvoiceBatchQueryServiceBlockingStub invoicingInvoiceBatchQueryServiceBlocking,
            InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking,
            InvoiceTemplateCalculationServiceGrpc.InvoiceTemplateCalculationServiceBlockingStub invoicingInvoiceTemplateCalculationServiceBlocking,
            InvoiceTemplateQueryServiceGrpc.InvoiceTemplateQueryServiceBlockingStub invoicingInvoiceTemplateQueryServiceBlocking,
            InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking,
            InvoiceBatchRequestMapper mapper,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceFutureStub notificationsNotificationValidationServiceFuture,
            PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking,
            InvoicingRepository repository,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.auditAuditTrailQueryServiceBlocking = auditAuditTrailQueryServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.events = events;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.invoicingEInvoiceCalculationServiceBlocking = invoicingEInvoiceCalculationServiceBlocking;
        this.invoicingEInvoiceValidationServiceBlocking = invoicingEInvoiceValidationServiceBlocking;
        this.invoicingInvoiceBatchCalculationServiceBlocking = invoicingInvoiceBatchCalculationServiceBlocking;
        this.invoicingInvoiceBatchQueryServiceBlocking = invoicingInvoiceBatchQueryServiceBlocking;
        this.invoicingInvoiceBatchValidationServiceBlocking = invoicingInvoiceBatchValidationServiceBlocking;
        this.invoicingInvoiceTemplateCalculationServiceBlocking = invoicingInvoiceTemplateCalculationServiceBlocking;
        this.invoicingInvoiceTemplateQueryServiceBlocking = invoicingInvoiceTemplateQueryServiceBlocking;
        this.invoicingInvoiceTemplateValidationServiceBlocking = invoicingInvoiceTemplateValidationServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.notificationsNotificationValidationServiceFuture = notificationsNotificationValidationServiceFuture;
        this.payrollPayRunCalculationServiceBlocking = payrollPayRunCalculationServiceBlocking;
        this.repository = repository;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    public record DeleteInvoiceBatchStage2Requested(SuggestAuditTrailsRequest request, CompletableFuture<SuggestAuditTrailsResponse> completion, io.grpc.Context context) {}

    public record WatchInvoiceBatchesStage1Requested(RecalculateBankTransfersRequest request, CompletableFuture<RecalculateBankTransfersResponse> completion, io.grpc.Context context) {}

    /**
     * Creates an invoice batch.
     */
    public CreateInvoiceBatchResponse createInvoiceBatch(CreateInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = CompareEInvoiceOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareEInvoiceOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getInvoiceBatch().getBillingAddress().getLocality())
                                .setRecipient(request.getInvoiceBatch().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setEInvoice(EInvoice.newBuilder()
                        .setCard(EInvoiceCardSettlement.newBuilder()
                                .setLast4(switch (request.getInvoiceBatch().getSettlementCase()) {
                                    case CARD -> request.getInvoiceBatch().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = invoicingEInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareEInvoiceOptions(outbound0);

        var outbound1 = ExportAuditTrailsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportAuditTrailsRequest.Filter.newBuilder()
                        .setRegion(reply0.getEInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = auditAuditTrailQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportAuditTrails(outbound1);
        var reply1 = ExportAuditTrailsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getCounterparty().getCustomerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateInvoiceBatch", request.getInvoiceBatch().getCounterparty().getCustomerId());
        return CreateInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatch().getInvoiceBatchId())
                        .setNumber(request.getInvoiceBatch().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns an invoice batch.
     */
    public GetInvoiceBatchResponse getInvoiceBatch(GetInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getInvoiceBatchId(), "GetInvoiceBatch");

        var outbound0 = ValidateInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateInvoiceBatchRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateInvoiceBatch(value)))
                .collect(Collectors.toList()).getFirst();
        return GetInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(reply0.getInvoiceBatch().getInvoiceBatchId())
                        .build())
                .build();
    }

    /**
     * Changes an invoice batch.
     */
    public UpdateInvoiceBatchResponse updateInvoiceBatch(UpdateInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = RpcSupport.unpack(Any.pack(SearchMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoiceBatch().getAudit().getCreatedAt())
                        .setRegion(request.getInvoiceBatch().getBillingAddress().getRegion())
                        .build())
                .build()), SearchMetricSeriesesRequest.class);
        var reply0 = metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchMetricSerieses(outbound0);

        var outbound1 = ExplainComplianceCaseCalculationRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getInvoiceBatch().getCounterparty().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getInvoiceBatch().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getInvoiceBatch().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<ExplainComplianceCaseCalculationRequest, ExplainComplianceCaseCalculationResponse> port1 = RpcPort.adapt(complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainComplianceCaseCalculation);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = QuoteInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getInvoiceBatch().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getInvoiceBatch().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply2 = invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteInvoiceBatch(outbound2);

        var outbound3 = CompareInvoiceTemplateOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareInvoiceTemplateOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getInvoiceBatch().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getInvoiceBatch().getAudit().getCreatedBy())
                                .setUpdatedBy(reply2.getInvoiceBatch().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var command3 = new DispatchCommand<>("acme.invoicing.v1.InvoiceTemplateCalculationService/CompareInvoiceTemplateOptions", outbound3);
        var reply3 = invoicingInvoiceTemplateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareInvoiceTemplateOptions(command3.payload());
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return UpdateInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getInvoiceBatch().getAudit().getCreatedBy())
                                .setEtag(reply1.getComplianceCase().getAudit().getEtag())
                                .setUpdatedAt(reply3.getInvoiceTemplate().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(request.getInvoiceBatch().getCounterparty())
                        .setIssuedAt(request.getInvoiceBatch().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes an invoice batch.
     */
    public DeleteInvoiceBatchResponse deleteInvoiceBatch(DeleteInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getInvoiceBatchId(), "DeleteInvoiceBatch");

        var outbound0 = (RecalculateCoveragesRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build()).toBuilder().build();
        var reply0 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCoverages(outbound0);

        var outbound1 = BeanCopies.copy(ValidateInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCoverage().getAudit().getEtag())
                                .setUpdatedBy(reply0.getCoverage().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), ValidateInvoiceTemplateRequest.class);
        var reply1 = invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateInvoiceTemplate(outbound1);

        var outbound2 = SuggestAuditTrailsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestAuditTrailsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getCoverage().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getInvoiceTemplate().getMetadataMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .setRegion(reply1.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion2 = new CompletableFuture<SuggestAuditTrailsResponse>();
        events.publishEvent(new DeleteInvoiceBatchStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = (QuotePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCoverage().getAudit().getUpdatedBy())
                                .build())
                        .addLines(PayRunLine.newBuilder()
                                .setTax(PayRunLineTax.newBuilder()
                                        .setAmount(reply0.getCoverage().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePayRun(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteInvoiceBatch", request.getInvoiceBatchId());
        return DeleteInvoiceBatchResponse.newBuilder()
                .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists invoice batches a page at a time.
     */
    public ListInvoiceBatchesResponse listInvoiceBatches(ListInvoiceBatchesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListInvoiceBatches")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getContext().getTraceId(), "ListInvoiceBatches");

        var outbound0 = ValidateNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = notificationsNotificationValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).validateNotification(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = (PreviewVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewVenueRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getNotification().getAssignee().getEmail())
                                .build())
                        .build())
                .setVenue(Venue.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getNotification().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getNotification().getRequester().getEmail())
                                .setUserId(reply0.getNotification().getAssignee().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewVenue(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListInvoiceBatches", request.getPage().getPageToken());
        return ListInvoiceBatchesResponse.newBuilder()
                .addInvoiceBatches(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getVenue().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getNotification().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Streams the changes of invoice batches as they happen.
     */
    public WatchInvoiceBatchesResponse watchInvoiceBatches(WatchInvoiceBatchesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchInvoiceBatches")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getContext().getTraceId(), "WatchInvoiceBatches");

        var outbound0 = mapper.watchInvoiceBatchesStage0(request);
        var reply0 = invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreInvoiceTemplate(outbound0);

        var outbound1 = RecalculateBankTransfersRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getInvoiceTemplate().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getInvoiceTemplate().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateBankTransfersRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getInvoiceTemplate().getBillingAddress().getLinesList())
                                .setLocality(reply0.getInvoiceTemplate().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<RecalculateBankTransfersResponse>();
        events.publishEvent(new WatchInvoiceBatchesStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = CheckInvoiceBatchEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getBankTransfer().getAudit().getUpdatedAt())
                                .build())
                        .setBank(InvoiceBatchBankSettlement.newBuilder()
                                .setBic(switch (reply0.getInvoiceTemplate().getSettlementCase()) {
                                    case BANK -> reply0.getInvoiceTemplate().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkInvoiceBatchEligibility(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchInvoiceBatches", request.getFilter().getQuery());
        return WatchInvoiceBatchesResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getInvoiceBatch().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(reply1.getBankTransfer().getCounterparty())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Takes a stream of invoice batches and answers once at the end.
     */
    public BatchCreateInvoiceBatchesResponse batchCreateInvoiceBatches(BatchCreateInvoiceBatchesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateInvoiceBatches")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = CountInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountInvoiceBatchesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRecord().getIssuedAt())
                        .putAllLabels(request.getRecord().getMetadataMap())
                        .setOwnerId(switch (request.getChunkCase()) {
                            case RECORD -> request.getRecord().getCounterparty().getCustomerId();
                            default -> "";
                        })
                        .setRegion(switch (request.getChunkCase()) {
                            case RECORD -> request.getRecord().getBillingAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply0 = invoicingInvoiceBatchQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countInvoiceBatches(outbound0);

        var outbound1 = LintInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintInvoiceBatchesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(switch (request.getChunkCase()) {
                                    case HEADER -> request.getHeader().getSubmittedBy().getEmail();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getAudit().getCreatedBy();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getBillingAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .setCard(InvoiceBatchCardSettlement.newBuilder()
                                .setNetwork(switch (request.getRecord().getSettlementCase()) {
                                    case CARD -> request.getRecord().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintInvoiceBatches(outbound1);

        List<LintEInvoicesResponse> results2 = request.getRecord().getLinesList().stream()
                .map(element -> LintEInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setEInvoice(EInvoice.newBuilder()
                        .addLines(EInvoiceLine.newBuilder()
                                .setDescription(element.getDescription())
                                .setTax(EInvoiceLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(invoicingEInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintEInvoices(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? LintEInvoicesResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateInvoiceBatches", request.getHeader().getSource());
        return BatchCreateInvoiceBatchesResponse.newBuilder()
                .addRejections(BatchCreateInvoiceBatchesResponse.Rejection.newBuilder()
                        .setInvoiceBatchId(reply1.getInvoiceBatch().getInvoiceBatchId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Creates an invoice batch.
     */
    public CloneInvoiceBatchResponse cloneInvoiceBatch(CloneInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = ResolveInvoiceTemplatesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> invoicingInvoiceTemplateQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveInvoiceTemplates(value)).orElseThrow();

        List<ExplainInvoiceTemplateDecisionResponse> results1 = request.getInvoiceBatch().getAttachmentsList().stream()
                .map(element -> ExplainInvoiceTemplateDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .addAttachments(InvoiceTemplateAttachment.newBuilder()
                                .setContentType(element.getContentType())
                                .setFileName(element.getFileName())
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainInvoiceTemplateDecision(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? ExplainInvoiceTemplateDecisionResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = NormalizeInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getInvoiceTemplate().getAudit().getCreatedAt())
                                .build())
                        .setBank(InvoiceTemplateBankSettlement.newBuilder()
                                .setAccountHolder(request.getInvoiceBatch().getBank().getAccountHolder())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.invoicing.v1.InvoiceTemplateValidationService");
        var selected2 = InvoiceTemplateValidationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.normalizeInvoiceTemplate(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return CloneInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getInvoiceTemplate().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(request.getInvoiceBatch().getCounterparty())
                        .setIssuedAt(request.getInvoiceBatch().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .build();
    }
}
