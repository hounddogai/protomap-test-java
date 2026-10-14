package com.acme.finance.domains.procurement;

import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.ListDepreciationReportsRequest;
import com.acme.assets.v1.ListDepreciationReportsResponse;
import com.acme.collections.v1.EscalateWriteOffRequest;
import com.acme.collections.v1.EscalateWriteOffResponse;
import com.acme.collections.v1.RecallWriteOffRequest;
import com.acme.collections.v1.RecallWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BrowseHouseholdsRequest;
import com.acme.customers.v1.BrowseHouseholdsResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExportCustomersRequest;
import com.acme.customers.v1.ExportCustomersResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FindJournalEntriesRequest;
import com.acme.ledger.v1.FindJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.procurement.v1.CountRfqTotalsRequest;
import com.acme.procurement.v1.CountRfqTotalsResponse;
import com.acme.procurement.v1.DeleteRfqReportRequest;
import com.acme.procurement.v1.DeleteRfqReportResponse;
import com.acme.procurement.v1.ExplainRfqCalculationRequest;
import com.acme.procurement.v1.ExplainRfqCalculationResponse;
import com.acme.procurement.v1.FetchRfqReportRequest;
import com.acme.procurement.v1.FetchRfqReportResponse;
import com.acme.procurement.v1.GenerateRfqReportRequest;
import com.acme.procurement.v1.GenerateRfqReportResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.ListRfqReportsRequest;
import com.acme.procurement.v1.ListRfqReportsResponse;
import com.acme.procurement.v1.NormalizeGoodsReceiptRequest;
import com.acme.procurement.v1.NormalizeGoodsReceiptResponse;
import com.acme.procurement.v1.PreviewGoodsReceiptRequest;
import com.acme.procurement.v1.PreviewGoodsReceiptResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.RfqSla;
import com.acme.procurement.v1.ScheduleRfqReportRequest;
import com.acme.procurement.v1.ScheduleRfqReportResponse;
import com.acme.procurement.v1.StreamRfqMetricsRequest;
import com.acme.procurement.v1.StreamRfqMetricsResponse;
import com.acme.procurement.v1.ValidatePurchaseRequisitionRequest;
import com.acme.procurement.v1.ValidatePurchaseRequisitionResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about rfqs. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class RfqReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(RfqReportingApplication.class);

    private final DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking;

    private final ChannelRegistry channels;

    private final WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final RfqReportingRequestMapper mapper;

    private final GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceBlockingStub procurementGoodsReceiptValidationServiceBlocking;

    private final GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceFutureStub procurementGoodsReceiptValidationServiceFuture;

    private final RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking;

    private final ProcurementRepository repository;

    public RfqReportingApplication(
            DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking,
            ChannelRegistry channels,
            WriteOffApprovalServiceGrpc.WriteOffApprovalServiceBlockingStub collectionsWriteOffApprovalServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            ApplicationEventPublisher events,
            RfqReportingRequestMapper mapper,
            GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceBlockingStub procurementGoodsReceiptValidationServiceBlocking,
            GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceFutureStub procurementGoodsReceiptValidationServiceFuture,
            RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking,
            ProcurementRepository repository) {
        this.assetsDepreciationReportingServiceBlocking = assetsDepreciationReportingServiceBlocking;
        this.channels = channels;
        this.collectionsWriteOffApprovalServiceBlocking = collectionsWriteOffApprovalServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.events = events;
        this.mapper = mapper;
        this.procurementGoodsReceiptValidationServiceBlocking = procurementGoodsReceiptValidationServiceBlocking;
        this.procurementGoodsReceiptValidationServiceFuture = procurementGoodsReceiptValidationServiceFuture;
        this.procurementRfqCalculationServiceBlocking = procurementRfqCalculationServiceBlocking;
        this.repository = repository;
    }

    public record GenerateRfqReportStage0Requested(BrowseHouseholdsRequest request, CompletableFuture<BrowseHouseholdsResponse> completion, io.grpc.Context context) {}

    public record GenerateRfqReportStage2Requested(ExplainRfqCalculationRequest request, CompletableFuture<ExplainRfqCalculationResponse> completion, io.grpc.Context context) {}

    /**
     * Generates a rfq.
     */
    public GenerateRfqReportResponse generateRfqReport(GenerateRfqReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateRfqReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getRfqId(), "GenerateRfqReport");

        var outbound0 = BrowseHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseHouseholdsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getRfqId())
                        .build())
                .build();
        var completion0 = new CompletableFuture<BrowseHouseholdsResponse>();
        events.publishEvent(new GenerateRfqReportStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = NormalizeGoodsReceiptRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(NormalizeGoodsReceiptRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending1 = procurementGoodsReceiptValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeGoodsReceipt(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = ExplainRfqCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getGoodsReceipt().getAudit().getCreatedBy())
                                .build())
                        .addComments(RfqComment.newBuilder()
                                .setCreatedAt(reply1.getGoodsReceipt().getAudit().getCreatedAt())
                                .build())
                        .setSla(RfqSla.newBuilder()
                                .setDueAt(reply1.getGoodsReceipt().getDueAt())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<ExplainRfqCalculationResponse>();
        events.publishEvent(new GenerateRfqReportStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateRfqReport", request.getDetails().getFormat());
        return GenerateRfqReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a rfq.
     */
    public FetchRfqReportResponse fetchRfqReport(FetchRfqReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchRfqReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getRfqId(), "FetchRfqReport");
        return FetchRfqReportResponse.newBuilder()
                .setRfq(Rfq.newBuilder()
                        .setRfqId(request.getRfqId())
                        .build())
                .build();
    }

    /**
     * Schedules a rfq.
     */
    public ScheduleRfqReportResponse scheduleRfqReport(ScheduleRfqReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleRfqReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getRfqId(), "ScheduleRfqReport");

        var outbound0 = RecallWriteOffRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(RecallWriteOffRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(collectionsWriteOffApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallWriteOff(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = FindJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindJournalEntriesRequest.Filter.newBuilder()
                        .setRegion(reply0.getWriteOff().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.ledger.v1.JournalEntryQueryService");
        var selected1 = JournalEntryQueryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.findJournalEntries(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleRfqReport", request.getRfqId());
        return ScheduleRfqReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Streams the changes of rfqs as they happen.
     */
    public StreamRfqMetricsResponse streamRfqMetrics(StreamRfqMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamRfqMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getContext().getTraceId(), "StreamRfqMetrics");

        var outbound0 = PreviewGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewGoodsReceiptRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setName(request.getSubscriber().getName())
                                .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                .build())
                        .build())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.procurement.v1.GoodsReceiptValidationService/PreviewGoodsReceipt", outbound0);
        var reply0 = procurementGoodsReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewGoodsReceipt(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamRfqMetrics", request.getFilter().getQuery());
        return StreamRfqMetricsResponse.newBuilder()
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getGoodsReceipt().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists rfqs a page at a time.
     */
    public ListRfqReportsResponse listRfqReports(ListRfqReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListRfqReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getContext().getTraceId(), "ListRfqReports");

        var outbound0 = BeanCopies.copy(ListDepreciationReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDepreciationReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build(), ListDepreciationReportsRequest.class);
        var reply0 = assetsDepreciationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listDepreciationReports(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListRfqReports", request.getFilter().getQuery());
        return ListRfqReportsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Counts rfqs by status.
     */
    public CountRfqTotalsResponse countRfqTotals(CountRfqTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountRfqTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getContext().getTraceId(), "CountRfqTotals");

        var outbound0 = mapper.countRfqTotalsStage0(request);
        var reply0 = procurementRfqCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainRfqCalculation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountRfqTotals", request.getPage().getPageToken());
        return CountRfqTotalsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes a rfq.
     */
    public DeleteRfqReportResponse deleteRfqReport(DeleteRfqReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteRfqReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Rfq", request.getRfqId(), "DeleteRfqReport");

        var builder0 = EscalateWriteOffRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setWriteOff(WriteOff.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setEtag(request.getEtag())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = collectionsWriteOffApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateWriteOff(outbound0);

        var outbound1 = ValidatePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getWriteOff().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.procurement.v1.PurchaseRequisitionValidationService");
        var selected1 = PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.validatePurchaseRequisition(outbound1);

        var outbound2 = ExportCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportCustomersRequest.Filter.newBuilder()
                        .setRegion(reply1.getPurchaseRequisition().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream2 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportCustomers(outbound2);
        var reply2 = ExportCustomersResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getRfqId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteRfqReport", request.getRfqId());
        return DeleteRfqReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }
}
