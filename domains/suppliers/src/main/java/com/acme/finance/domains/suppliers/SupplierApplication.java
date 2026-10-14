package com.acme.finance.domains.suppliers;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.ScoreDisposalRequest;
import com.acme.assets.v1.ScoreDisposalResponse;
import com.acme.assets.v1.SearchFixedAssetsRequest;
import com.acme.assets.v1.SearchFixedAssetsResponse;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.ShareAuditTrailExportRequest;
import com.acme.audit.v1.ShareAuditTrailExportResponse;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.ExportBudgetLinesRequest;
import com.acme.budgeting.v1.ExportBudgetLinesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.NormalizeObligationRequest;
import com.acme.contracts.v1.NormalizeObligationResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.ResolveClaimsRequest;
import com.acme.insurance.v1.ResolveClaimsResponse;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.suppliers.v1.BatchCreateSuppliersRequest;
import com.acme.suppliers.v1.BatchCreateSuppliersResponse;
import com.acme.suppliers.v1.BatchGetSuppliersRequest;
import com.acme.suppliers.v1.BatchGetSuppliersResponse;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.CountSuppliersRequest;
import com.acme.suppliers.v1.CountSuppliersResponse;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.DeleteSupplierRequest;
import com.acme.suppliers.v1.DeleteSupplierResponse;
import com.acme.suppliers.v1.FindSupplierContractsRequest;
import com.acme.suppliers.v1.FindSupplierContractsResponse;
import com.acme.suppliers.v1.GetSupplierRequest;
import com.acme.suppliers.v1.GetSupplierResponse;
import com.acme.suppliers.v1.LintSupplierRatingsRequest;
import com.acme.suppliers.v1.LintSupplierRatingsResponse;
import com.acme.suppliers.v1.ListSuppliersRequest;
import com.acme.suppliers.v1.ListSuppliersResponse;
import com.acme.suppliers.v1.ResolveSupplierContractsRequest;
import com.acme.suppliers.v1.ResolveSupplierContractsResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.UpdateSupplierRequest;
import com.acme.suppliers.v1.UpdateSupplierResponse;
import com.acme.suppliers.v1.WatchSuppliersRequest;
import com.acme.suppliers.v1.WatchSuppliersResponse;
import com.acme.yards.v1.ExplainYardCalculationRequest;
import com.acme.yards.v1.ExplainYardCalculationResponse;
import com.acme.yards.v1.Yard;
import com.acme.yards.v1.YardCalculationServiceGrpc;
import com.google.protobuf.Any;
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
 * Manages suppliers: creates, reads, changes, and lists them. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class SupplierApplication {
    private static final Logger log = LoggerFactory.getLogger(SupplierApplication.class);

    private final DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking;

    private final FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking;

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final BudgetLineQueryServiceGrpc.BudgetLineQueryServiceBlockingStub budgetingBudgetLineQueryServiceBlocking;

    private final ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking;

    private final ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final SuppliersRepository repository;

    private final Executor rpcExecutor;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final YardCalculationServiceGrpc.YardCalculationServiceBlockingStub yardsYardCalculationServiceBlocking;

    public SupplierApplication(
            DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking,
            FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking,
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            BudgetLineQueryServiceGrpc.BudgetLineQueryServiceBlockingStub budgetingBudgetLineQueryServiceBlocking,
            ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking,
            ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            SuppliersRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            YardCalculationServiceGrpc.YardCalculationServiceBlockingStub yardsYardCalculationServiceBlocking) {
        this.assetsDisposalValidationServiceBlocking = assetsDisposalValidationServiceBlocking;
        this.assetsFixedAssetServiceBlocking = assetsFixedAssetServiceBlocking;
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.budgetingBudgetLineQueryServiceBlocking = budgetingBudgetLineQueryServiceBlocking;
        this.contractsObligationValidationServiceBlocking = contractsObligationValidationServiceBlocking;
        this.insuranceClaimQueryServiceBlocking = insuranceClaimQueryServiceBlocking;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.yardsYardCalculationServiceBlocking = yardsYardCalculationServiceBlocking;
    }

    /**
     * Creates a supplier.
     */
    public CreateSupplierResponse createSupplier(CreateSupplierRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateSupplier")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplier(request.getContext().getTenantId(), request.getSupplier());

        var outbound0 = SearchFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchFixedAssetsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplier().getPassport().getExpiresAt())
                        .putAllLabels(request.getSupplier().getAttributesMap())
                        .setRegion(request.getSupplier().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchFixedAssets(outbound0), rpcExecutor);

        var outbound1 = ResolveSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveSupplierContracts(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getSupplier().getContact().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateSupplier", request.getSupplier().getContact().getEmail());
        return CreateSupplierResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setSupplier(Supplier.newBuilder()
                        .setRelationshipManager(request.getSupplier().getRelationshipManager())
                        .setSupplierId(request.getSupplier().getSupplierId())
                        .build())
                .build();
    }

    /**
     * Returns a supplier.
     */
    public GetSupplierResponse getSupplier(GetSupplierRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetSupplier")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getSupplierId(), "GetSupplier");
        return GetSupplierResponse.newBuilder()
                .setSupplier(Supplier.newBuilder()
                        .setSupplierId(request.getSupplierId())
                        .build())
                .build();
    }

    /**
     * Changes a supplier.
     */
    public UpdateSupplierResponse updateSupplier(UpdateSupplierRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateSupplier")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplier(request.getContext().getTenantId(), request.getSupplier());

        var outbound0 = ExportBudgetLinesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportBudgetLinesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplier().getPassport().getExpiresAt())
                        .setRegion(request.getSupplier().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var stream0 = budgetingBudgetLineQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportBudgetLines(outbound0);
        var reply0 = ExportBudgetLinesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = ResolveClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply1 = insuranceClaimQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveClaims(outbound1);

        var outbound2 = NormalizeObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeObligationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSupplier().getContact().getEmail())
                                .build())
                        .build())
                .setObligation(Obligation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getSupplier().getAudit().getUpdatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getSupplier().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply2 = contractsObligationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeObligation(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSupplier().getSupplierId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateSupplier", request.getSupplier().getSupplierId());
        return UpdateSupplierResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .setSupplier(Supplier.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply2.getObligation().getAssignee().getEmail())
                                .setPreferredChannel(reply2.getObligation().getRequester().getPreferredChannel())
                                .build())
                        .setRelationshipManager(request.getSupplier().getRelationshipManager())
                        .setStatus(request.getSupplier().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes a supplier.
     */
    public DeleteSupplierResponse deleteSupplier(DeleteSupplierRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteSupplier")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getSupplierId(), "DeleteSupplier");

        var outbound0 = RpcSupport.unpack(Any.pack(FindSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindSupplierContractsRequest.Filter.newBuilder()
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getContext().getActor().getUserId())
                        .build())
                .build()), FindSupplierContractsRequest.class);
        var reply0 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findSupplierContracts(outbound0);

        var outbound1 = CountSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSuppliersRequest.Filter.newBuilder()
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getSupplierId())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSuppliers(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getReason()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteSupplier", request.getReason());
        return DeleteSupplierResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists suppliers a page at a time.
     */
    public ListSuppliersResponse listSuppliers(ListSuppliersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListSuppliers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getContext().getTraceId(), "ListSuppliers");

        var outbound0 = ScoreDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreDisposalRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = assetsDisposalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreDisposal(outbound0);

        var outbound1 = ListMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListMetricSeriesesRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build();
        Function<ListMetricSeriesesRequest, ListMetricSeriesesResponse> operation1 = metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listMetricSerieses;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getOwnerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListSuppliers", request.getFilter().getOwnerId());
        return ListSuppliersResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply1.getPageInfo().getNextPageToken())
                        .build())
                .addSuppliers(Supplier.newBuilder()
                        .setRelationshipManager(UserRef.newBuilder()
                                .setUserId(reply0.getDisposal().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns several suppliers at once.
     */
    public BatchGetSuppliersResponse batchGetSuppliers(BatchGetSuppliersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetSuppliers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getContext().getTraceId(), "BatchGetSuppliers");
        return BatchGetSuppliersResponse.getDefaultInstance();
    }

    /**
     * Streams the changes of suppliers as they happen.
     */
    public WatchSuppliersResponse watchSuppliers(WatchSuppliersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchSuppliers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getContext().getTraceId(), "WatchSuppliers");

        var outbound0 = LintSupplierRatingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintSupplierRatingsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getSubscriber().getName())
                                .setPhone(request.getSubscriber().getPhone())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintSupplierRatings(value));
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchSuppliers", request.getFilter().getQuery());
        return WatchSuppliersResponse.newBuilder()
                .setSupplier(Supplier.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getSubscriber().getName())
                                .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getSupplierRating().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
    }

    /**
     * Takes a stream of suppliers and answers once at the end.
     */
    public BatchCreateSuppliersResponse batchCreateSuppliers(BatchCreateSuppliersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateSuppliers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveSupplier(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = ExplainYardCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainYardCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getRecord().getContact().getPhone())
                                .build())
                        .build())
                .setYard(Yard.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getRecord().getAudit().getEtag())
                                .setUpdatedBy(request.getRecord().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> yardsYardCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainYardCalculation(outbound0), rpcExecutor);

        var outbound1 = ShareAuditTrailExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareAuditTrailExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getHeader().getSubmittedBy().getEmail())
                                .setName(request.getRecord().getContact().getName())
                                .setPhone(request.getHeader().getSubmittedBy().getPhone())
                                .setPreferredChannel(request.getHeader().getSubmittedBy().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareAuditTrailExport(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = BrowseSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSupplierContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getOutcome().getEffectiveAt())
                        .putAllLabels(reply1.getAuditTrail().getMetadataMap())
                        .setOwnerId(switch (request.getChunkCase()) {
                            case RECORD -> request.getRecord().getRelationshipManager().getUserId();
                            default -> "";
                        })
                        .setRegion(reply1.getAuditTrail().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSupplierContracts(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateSuppliers", request.getHeader().getSource());
        return BatchCreateSuppliersResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }
}
