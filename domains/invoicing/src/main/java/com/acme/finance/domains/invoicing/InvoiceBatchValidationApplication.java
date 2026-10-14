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
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.CalculateInvoiceBatchRequest;
import com.acme.invoicing.v1.CalculateInvoiceBatchResponse;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityResponse;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsResponse;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsResponse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Checks invoice batches against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class InvoiceBatchValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceBatchValidationApplication.class);

    private final AnomalyServiceGrpc.AnomalyServiceBlockingStub anomaliesAnomalyServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final ChannelRegistry channels;

    private final ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking;

    private final InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking;

    private final InvoiceTemplateCalculationServiceGrpc.InvoiceTemplateCalculationServiceBlockingStub invoicingInvoiceTemplateCalculationServiceBlocking;

    private final InvoiceBatchValidationRequestMapper mapper;

    private final PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceFutureStub procurementPurchaseRequisitionQueryServiceFuture;

    private final InvoicingRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public InvoiceBatchValidationApplication(
            AnomalyServiceGrpc.AnomalyServiceBlockingStub anomaliesAnomalyServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            ChannelRegistry channels,
            ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking,
            InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking,
            InvoiceTemplateCalculationServiceGrpc.InvoiceTemplateCalculationServiceBlockingStub invoicingInvoiceTemplateCalculationServiceBlocking,
            InvoiceBatchValidationRequestMapper mapper,
            PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceFutureStub procurementPurchaseRequisitionQueryServiceFuture,
            InvoicingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.anomaliesAnomalyServiceBlocking = anomaliesAnomalyServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.channels = channels;
        this.expensesReceiptCalculationServiceBlocking = expensesReceiptCalculationServiceBlocking;
        this.invoicingInvoiceBatchCalculationServiceBlocking = invoicingInvoiceBatchCalculationServiceBlocking;
        this.invoicingInvoiceTemplateCalculationServiceBlocking = invoicingInvoiceTemplateCalculationServiceBlocking;
        this.mapper = mapper;
        this.procurementPurchaseRequisitionQueryServiceFuture = procurementPurchaseRequisitionQueryServiceFuture;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    /**
     * Validates an invoice batch.
     */
    public ValidateInvoiceBatchResponse validateInvoiceBatch(ValidateInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = SummarizePurchaseRequisitionsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizePurchaseRequisitionsRequest.Details.newBuilder()
                        .putAllFilters(request.getInvoiceBatch().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = procurementPurchaseRequisitionQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).summarizePurchaseRequisitions(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return ValidateInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setTotal(request.getInvoiceBatch().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews an invoice batch.
     */
    public PreviewInvoiceBatchResponse previewInvoiceBatch(PreviewInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = ListRetentionPolicySubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRetentionPolicySubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoiceBatch().getAudit().getUpdatedAt())
                        .putAllLabels(request.getInvoiceBatch().getMetadataMap())
                        .setRegion(request.getInvoiceBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        var adapter0 = new Function<ListRetentionPolicySubscriptionsRequest, ListRetentionPolicySubscriptionsResponse>() {
            @Override
            public ListRetentionPolicySubscriptionsResponse apply(ListRetentionPolicySubscriptionsRequest input) {
                return auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRetentionPolicySubscriptions(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        List<SimulateInvoiceBatchResponse> results1 = request.getInvoiceBatch().getAttachmentsList().stream()
                .map(element -> SimulateInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .addAttachments(InvoiceBatchAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setFileName(element.getFileName())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateInvoiceBatch(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? SimulateInvoiceBatchResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = JsonCodec.roundTrip(CreateAnomalyRequest.newBuilder()
                .setAnomaly(Anomaly.newBuilder()
                        .addComments(AnomalyComment.newBuilder()
                                .setCreatedAt(request.getInvoiceBatch().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), CreateAnomalyRequest.class);
        var reply2 = anomaliesAnomalyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createAnomaly(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return PreviewInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getAnomaly().getAudit().getCreatedBy())
                                .build())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setIssuedAt(request.getInvoiceBatch().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks an invoice batch.
     */
    public CheckInvoiceBatchEligibilityResponse checkInvoiceBatchEligibility(CheckInvoiceBatchEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckInvoiceBatchEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = EstimateReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateReceiptRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getInvoiceBatch().getBillingAddress().getPostalCode())
                                .setRegion(request.getInvoiceBatch().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getInvoiceBatch().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            EstimateReceiptResponse fetch(EstimateReceiptRequest input) {
                return expensesReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateReceipt(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = mapper.checkInvoiceBatchEligibilityStage1(request, reply0);
        var reply1 = searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateSearchIndex(outbound1);

        List<EstimateInvoiceBatchResponse> results2 = request.getInvoiceBatch().getLinesList().stream()
                .map(element -> EstimateInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .addLines(InvoiceBatchLine.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .setSku(element.getSku())
                                .setTax(InvoiceBatchLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateInvoiceBatch(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? EstimateInvoiceBatchResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckInvoiceBatchEligibility", request.getInvoiceBatch().getInvoiceBatchId());
        return CheckInvoiceBatchEligibilityResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setIssuedAt(request.getInvoiceBatch().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Normalizes an invoice batch.
     */
    public NormalizeInvoiceBatchResponse normalizeInvoiceBatch(NormalizeInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = NormalizeSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getInvoiceBatch().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        Function<NormalizeSearchIndexRequest, NormalizeSearchIndexResponse> operation0 = searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeSearchIndex;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = SearchVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchVenuesRequest.Filter.newBuilder()
                        .setRegion(request.getInvoiceBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchVenues(outbound1);

        var outbound2 = RecalculateInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getSearchIndex().getAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<RecalculateInvoiceBatchesRequest, RecalculateInvoiceBatchesResponse>() {
            @Override
            public RecalculateInvoiceBatchesResponse apply(RecalculateInvoiceBatchesRequest input) {
                return invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateInvoiceBatches(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);

        var outbound3 = GetInvoiceBatchRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatchId(request.getInvoiceBatchId())
                .build();
        RpcPort<GetInvoiceBatchRatesRequest, GetInvoiceBatchRatesResponse> port3 = RpcPort.adapt(invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::getInvoiceBatchRates);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return NormalizeInvoiceBatchResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply2.getInvoiceBatch().getBillingAddress().getLocality())
                                .setRegion(reply3.getInvoiceBatch().getBillingAddress().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply2.getInvoiceBatch().getCounterparty().getEmail())
                                .build())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .putAllMetadata(reply3.getInvoiceBatch().getMetadataMap())
                        .setTotal(request.getInvoiceBatch().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Explains an invoice batch.
     */
    public ExplainInvoiceBatchDecisionResponse explainInvoiceBatchDecision(ExplainInvoiceBatchDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainInvoiceBatchDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = BeanCopies.copy(UnsubscribeRetentionPolicyUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeRetentionPolicyUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build(), UnsubscribeRetentionPolicyUpdatesRequest.class);
        var reply0 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeRetentionPolicyUpdates(outbound0);

        List<QuoteInvoiceBatchResponse> results1 = request.getInvoiceBatch().getAttachmentsList().stream()
                .map(element -> QuoteInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .addAttachments(InvoiceBatchAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteInvoiceBatch(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? QuoteInvoiceBatchResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = CompareInvoiceTemplateOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareInvoiceTemplateOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getInvoiceBatch().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getInvoiceBatch().getAudit().getCreatedAt())
                                .build())
                        .setBank(InvoiceTemplateBankSettlement.newBuilder()
                                .setBic(switch (reply1.getInvoiceBatch().getSettlementCase()) {
                                    case BANK -> reply1.getInvoiceBatch().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = invoicingInvoiceTemplateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareInvoiceTemplateOptions(outbound2);

        var outbound3 = mapper.explainInvoiceBatchDecisionStage3(request);
        var reply3 = invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getInvoiceBatchRates(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainInvoiceBatchDecision", request.getInvoiceBatchId());
        return ExplainInvoiceBatchDecisionResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRetentionPolicy().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(request.getInvoiceBatch().getCounterparty())
                        .setInvoiceBatchId(reply3.getInvoiceBatch().getInvoiceBatchId())
                        .setNumber(request.getInvoiceBatch().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Scores an invoice batch.
     */
    public ScoreInvoiceBatchResponse scoreInvoiceBatch(ScoreInvoiceBatchRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreInvoiceBatch")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = RecalculateInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateInvoiceBatchesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getInvoiceBatch().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getInvoiceBatch().getBillingAddress().getLinesList())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build();
        RpcPort<RecalculateInvoiceBatchesRequest, RecalculateInvoiceBatchesResponse> port0 = RpcPort.adapt(invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateInvoiceBatches);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = SimulateInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateInvoiceBatchRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getInvoiceBatch().getBillingAddress().getLinesList())
                                .setLocality(reply0.getInvoiceBatch().getBillingAddress().getLocality())
                                .setPostalCode(request.getInvoiceBatch().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setCard(InvoiceBatchCardSettlement.newBuilder()
                                .setHolderName(switch (reply0.getInvoiceBatch().getSettlementCase()) {
                                    case CARD -> reply0.getInvoiceBatch().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateInvoiceBatch(outbound1);

        var outbound2 = GetInvoiceBatchRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatchId(request.getInvoiceBatchId())
                .build();
        var channel2 = channels.lookup("acme.invoicing.v1.InvoiceBatchCalculationService");
        var selected2 = InvoiceBatchCalculationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.getInvoiceBatchRates(outbound2);

        var outbound3 = CalculateInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply2.getInvoiceBatch().getBillingAddress().getLocality())
                                .build())
                        .setCard(InvoiceBatchCardSettlement.newBuilder()
                                .setLast4(switch (request.getInvoiceBatch().getSettlementCase()) {
                                    case CARD -> request.getInvoiceBatch().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(request.getInvoiceBatch().getCounterparty().getCustomerId())
                                .build())
                        .build())
                .build();
        var reply3 = invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateInvoiceBatch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreInvoiceBatch", request.getInvoiceBatch().getInvoiceBatchId());
        return ScoreInvoiceBatchResponse.newBuilder()
                .setBreakdown(ScoreInvoiceBatchResponse.Breakdown.newBuilder()
                        .setTotal(request.getInvoiceBatch().getTotal())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getInvoiceBatch().getAudit().getEtag())
                                .build())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setNumber(reply3.getInvoiceBatch().getNumber())
                        .setTotal(reply2.getInvoiceBatch().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lints an invoice batch.
     */
    public LintInvoiceBatchesResponse lintInvoiceBatches(LintInvoiceBatchesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintInvoiceBatches")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var futures0 = request.getInvoiceBatch().getAttachmentsList().stream()
                .map(element -> CompareInvoiceBatchOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .addAttachments(InvoiceBatchAttachment.newBuilder()
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareInvoiceBatchOptions(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? CompareInvoiceBatchOptionsResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintInvoiceBatches", request.getInvoiceBatch().getInvoiceBatchId());
        return LintInvoiceBatchesResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .putAllMetadata(request.getInvoiceBatch().getMetadataMap())
                        .build())
                .build();
    }
}
