package com.acme.finance.domains.procurement;

import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.ListAuditEventVersionsRequest;
import com.acme.audit.v1.ListAuditEventVersionsResponse;
import com.acme.audit.v1.ListAuditTrailVersionsRequest;
import com.acme.audit.v1.ListAuditTrailVersionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BatchGetBudgetsRequest;
import com.acme.budgeting.v1.BatchGetBudgetsResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.budgeting.v1.QuoteBudgetLineRequest;
import com.acme.budgeting.v1.QuoteBudgetLineResponse;
import com.acme.budgeting.v1.RecalculateVariancesRequest;
import com.acme.budgeting.v1.RecalculateVariancesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.BrowseComplianceCasesRequest;
import com.acme.compliance.v1.BrowseComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.BrowseLoansRequest;
import com.acme.lending.v1.BrowseLoansResponse;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.metrics.v1.BatchCreateMetricSeriesesRequest;
import com.acme.metrics.v1.BatchCreateMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.procurement.v1.CalculateGoodsReceiptRequest;
import com.acme.procurement.v1.CalculateGoodsReceiptResponse;
import com.acme.procurement.v1.CalculatePurchaseOrderRequest;
import com.acme.procurement.v1.CalculatePurchaseOrderResponse;
import com.acme.procurement.v1.CompareGoodsReceiptOptionsRequest;
import com.acme.procurement.v1.CompareGoodsReceiptOptionsResponse;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsResponse;
import com.acme.procurement.v1.EstimateGoodsReceiptRequest;
import com.acme.procurement.v1.EstimateGoodsReceiptResponse;
import com.acme.procurement.v1.ExplainGoodsReceiptCalculationRequest;
import com.acme.procurement.v1.ExplainGoodsReceiptCalculationResponse;
import com.acme.procurement.v1.GetGoodsReceiptRatesRequest;
import com.acme.procurement.v1.GetGoodsReceiptRatesResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.NormalizePurchaseRequisitionRequest;
import com.acme.procurement.v1.NormalizePurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderBankSettlement;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionAttachment;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionCardSettlement;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.QuoteGoodsReceiptRequest;
import com.acme.procurement.v1.QuoteGoodsReceiptResponse;
import com.acme.procurement.v1.QuotePurchaseOrderRequest;
import com.acme.procurement.v1.QuotePurchaseOrderResponse;
import com.acme.procurement.v1.RecalculateGoodsReceiptsRequest;
import com.acme.procurement.v1.RecalculateGoodsReceiptsResponse;
import com.acme.procurement.v1.ScorePurchaseRequisitionRequest;
import com.acme.procurement.v1.ScorePurchaseRequisitionResponse;
import com.acme.procurement.v1.SimulateGoodsReceiptRequest;
import com.acme.procurement.v1.SimulateGoodsReceiptResponse;
import com.acme.procurement.v1.SimulatePurchaseOrderRequest;
import com.acme.procurement.v1.SimulatePurchaseOrderResponse;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityResponse;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
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
 * Computes the figures of a goods receipt, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class GoodsReceiptCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(GoodsReceiptCalculationApplication.class);

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync;

    private final BudgetServiceGrpc.BudgetServiceBlockingStub budgetingBudgetServiceBlocking;

    private final ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceBlockingStub complianceComplianceCaseQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking;

    private final LoanQueryServiceGrpc.LoanQueryServiceBlockingStub lendingLoanQueryServiceBlocking;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceStub metricsMetricSeriesServiceAsync;

    private final PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking;

    private final PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking;

    private final PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    public GoodsReceiptCalculationApplication(
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync,
            BudgetServiceGrpc.BudgetServiceBlockingStub budgetingBudgetServiceBlocking,
            ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceBlockingStub complianceComplianceCaseQueryServiceBlocking,
            ApplicationEventPublisher events,
            FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking,
            LoanQueryServiceGrpc.LoanQueryServiceBlockingStub lendingLoanQueryServiceBlocking,
            MetricSeriesServiceGrpc.MetricSeriesServiceStub metricsMetricSeriesServiceAsync,
            PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking,
            PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking,
            PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking) {
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.budgetingBudgetServiceAsync = budgetingBudgetServiceAsync;
        this.budgetingBudgetServiceBlocking = budgetingBudgetServiceBlocking;
        this.complianceComplianceCaseQueryServiceBlocking = complianceComplianceCaseQueryServiceBlocking;
        this.events = events;
        this.featureflagsFeatureFlagServiceBlocking = featureflagsFeatureFlagServiceBlocking;
        this.lendingLoanQueryServiceBlocking = lendingLoanQueryServiceBlocking;
        this.metricsMetricSeriesServiceAsync = metricsMetricSeriesServiceAsync;
        this.procurementPurchaseOrderCalculationServiceBlocking = procurementPurchaseOrderCalculationServiceBlocking;
        this.procurementPurchaseRequisitionCalculationServiceBlocking = procurementPurchaseRequisitionCalculationServiceBlocking;
        this.procurementPurchaseRequisitionValidationServiceBlocking = procurementPurchaseRequisitionValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
    }

    public record QuoteGoodsReceiptStage1Requested(ListAuditEventVersionsRequest request, CompletableFuture<ListAuditEventVersionsResponse> completion, io.grpc.Context context) {}

    public record CompareGoodsReceiptOptionsStage3Requested(RecalculateVariancesRequest request, CompletableFuture<RecalculateVariancesResponse> completion, io.grpc.Context context) {}

    public record RecalculateGoodsReceiptsStage1Requested(QuoteBudgetLineRequest request, CompletableFuture<QuoteBudgetLineResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates a goods receipt.
     */
    public CalculateGoodsReceiptResponse calculateGoodsReceipt(CalculateGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var results0 = new ArrayList<NormalizePurchaseRequisitionResponse>();
        for (var element : request.getGoodsReceipt().getAttachmentsList()) {
            NormalizePurchaseRequisitionRequest itemRequest = NormalizePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .addAttachments(PurchaseRequisitionAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .build())
                        .build())
                .build();
            results0.add(procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePurchaseRequisition(itemRequest));
        }
        var reply0 = results0.isEmpty() ? NormalizePurchaseRequisitionResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = BatchCreateBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBudgetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateBudgetsResponse.getDefaultInstance());
        var input1 = budgetingBudgetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBudgets(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getGoodsReceipt().getAudit().getCreatedAt())
                                .setCreatedBy(request.getGoodsReceipt().getAudit().getCreatedBy())
                                .setEtag(request.getGoodsReceipt().getAudit().getEtag())
                                .setUpdatedAt(request.getGoodsReceipt().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateGoodsReceipt", request.getGoodsReceipt().getGoodsReceiptId());
        return CalculateGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setNotes(request.getGoodsReceipt().getNotes())
                        .build())
                .build();
    }

    /**
     * Estimates a goods receipt.
     */
    public EstimateGoodsReceiptResponse estimateGoodsReceipt(EstimateGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = BatchGetFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<BatchGetFeatureFlagsRequest, BatchGetFeatureFlagsResponse>() {
            @Override
            public BatchGetFeatureFlagsResponse apply(BatchGetFeatureFlagsRequest input) {
                return featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetFeatureFlags(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateGoodsReceipt", request.getGoodsReceipt().getGoodsReceiptId());
        return EstimateGoodsReceiptResponse.newBuilder()
                .setBreakdown(EstimateGoodsReceiptResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Quotes a goods receipt.
     */
    public QuoteGoodsReceiptResponse quoteGoodsReceipt(QuoteGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        List<BrowseComplianceCasesResponse> results0 = request.getGoodsReceipt().getLinesList().stream()
                .map(element -> BrowseComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseComplianceCasesRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getGoodsReceipt().getBillingAddress().getRegion())
                        .build())
                .build())
                .flatMap(value -> Stream.of(complianceComplianceCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseComplianceCases(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? BrowseComplianceCasesResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListAuditEventVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getGoodsReceipt().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .setRegion(request.getGoodsReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ListAuditEventVersionsResponse>();
        events.publishEvent(new QuoteGoodsReceiptStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = BeanCopies.copy(ListAuditTrailVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailVersionsRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build(), ListAuditTrailVersionsRequest.class);
        var reply2 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditTrailVersions(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteGoodsReceipt", request.getGoodsReceipt().getGoodsReceiptId());
        return QuoteGoodsReceiptResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setTotal(request.getGoodsReceipt().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Compares a goods receipt.
     */
    public CompareGoodsReceiptOptionsResponse compareGoodsReceiptOptions(CompareGoodsReceiptOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareGoodsReceiptOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = BrowseLoansRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLoansRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getGoodsReceipt().getIssuedAt())
                        .setRegion(request.getGoodsReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        RpcPort<BrowseLoansRequest, BrowseLoansResponse> port0 = RpcPort.adapt(lendingLoanQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::browseLoans);
        var reply0 = port0.exchange(outbound0);

        var results1 = new ArrayList<ComparePurchaseRequisitionOptionsResponse>();
        for (var element : request.getGoodsReceipt().getAttachmentsList()) {
            ComparePurchaseRequisitionOptionsRequest itemRequest = ComparePurchaseRequisitionOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .addAttachments(PurchaseRequisitionAttachment.newBuilder()
                                .setContentType(element.getContentType())
                                .setFileName(element.getFileName())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build();
            results1.add(procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePurchaseRequisitionOptions(itemRequest));
        }
        var reply1 = results1.isEmpty() ? ComparePurchaseRequisitionOptionsResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = SimulatePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulatePurchaseOrderRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply1.getPurchaseRequisition().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        Function<SimulatePurchaseOrderRequest, SimulatePurchaseOrderResponse> operation2 = procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::simulatePurchaseOrder;
        var reply2 = operation2.apply(outbound2);

        var outbound3 = RecalculateVariancesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateVariancesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getPurchaseRequisition().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setVariance(Variance.newBuilder()
                        .addComments(VarianceComment.newBuilder()
                                .setCreatedAt(reply1.getPurchaseRequisition().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var completion3 = new CompletableFuture<RecalculateVariancesResponse>();
        events.publishEvent(new CompareGoodsReceiptOptionsStage3Requested(outbound3, completion3, io.grpc.Context.current()));
        var reply3 = completion3.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareGoodsReceiptOptions", request.getGoodsReceipt().getGoodsReceiptId());
        return CompareGoodsReceiptOptionsResponse.newBuilder()
                .setBreakdown(CompareGoodsReceiptOptionsResponse.Breakdown.newBuilder()
                        .setTotal(reply2.getBreakdown().getTotal())
                        .build())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply3.getVariance().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(request.getGoodsReceipt().getNumber())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Recalculates a goods receipt.
     */
    public RecalculateGoodsReceiptsResponse recalculateGoodsReceipts(RecalculateGoodsReceiptsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateGoodsReceipts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = BatchCreateMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setRecord(MetricSeries.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getGoodsReceipt().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(BatchCreateMetricSeriesesResponse.getDefaultInstance());
        var input0 = metricsMetricSeriesServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateMetricSerieses(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();

        var outbound1 = QuoteBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getGoodsReceipt().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteBudgetLineRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getGoodsReceipt().getBillingAddress().getCountryCode())
                                .setRegion(request.getGoodsReceipt().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<QuoteBudgetLineResponse>();
        events.publishEvent(new RecalculateGoodsReceiptsStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateGoodsReceipts", request.getGoodsReceipt().getGoodsReceiptId());
        return RecalculateGoodsReceiptsResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setIssuedAt(request.getGoodsReceipt().getIssuedAt())
                        .putAllMetadata(reply1.getBudgetLine().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Simulates a goods receipt.
     */
    public SimulateGoodsReceiptResponse simulateGoodsReceipt(SimulateGoodsReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateGoodsReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = CalculatePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getGoodsReceipt().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePurchaseOrder(outbound0), rpcExecutor);

        var outbound1 = BatchGetBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> budgetingBudgetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetBudgets(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = (ScorePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScorePurchaseRequisitionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPurchaseOrder().getBillingAddress().getCountryCode())
                                .setRecipient(request.getGoodsReceipt().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setCard(PurchaseRequisitionCardSettlement.newBuilder()
                                .setHolderName(reply0.getPurchaseOrder().getCard().getHolderName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scorePurchaseRequisition(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateGoodsReceipt", request.getGoodsReceipt().getGoodsReceiptId());
        return SimulateGoodsReceiptResponse.newBuilder()
                .setBreakdown(SimulateGoodsReceiptResponse.Breakdown.newBuilder()
                        .setTotal(reply2.getBreakdown().getTotal())
                        .build())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply2.getPurchaseRequisition().getBillingAddress().getRegion())
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(reply0.getPurchaseOrder().getCounterparty().getNumber())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setNumber(request.getGoodsReceipt().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Explains a goods receipt.
     */
    public ExplainGoodsReceiptCalculationResponse explainGoodsReceiptCalculation(ExplainGoodsReceiptCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainGoodsReceiptCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveGoodsReceipt(request.getContext().getTenantId(), request.getGoodsReceipt());

        var outbound0 = QuotePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePurchaseOrderRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getGoodsReceipt().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBank(PurchaseOrderBankSettlement.newBuilder()
                                .setAccountHolder(request.getGoodsReceipt().getBank().getAccountHolder())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePurchaseOrder(outbound0), rpcExecutor);

        var outbound1 = CheckSupplierRiskEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getGoodsReceipt().getCounterparty().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getGoodsReceipt().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getGoodsReceipt().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkSupplierRiskEligibility(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getGoodsReceipt().getGoodsReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainGoodsReceiptCalculation", request.getGoodsReceipt().getGoodsReceiptId());
        return ExplainGoodsReceiptCalculationResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(reply0.getPurchaseOrder().getCounterparty().getDisplayName())
                                .build())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .setNumber(request.getGoodsReceipt().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a goods receipt.
     */
    public GetGoodsReceiptRatesResponse getGoodsReceiptRates(GetGoodsReceiptRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetGoodsReceiptRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "GoodsReceipt", request.getGoodsReceiptId(), "GetGoodsReceiptRates");

        var outbound0 = ScorePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setIssuedAt(request.getContext().getDeadline())
                        .build())
                .build();
        var adapter0 = new Function<ScorePurchaseRequisitionRequest, ScorePurchaseRequisitionResponse>() {
            @Override
            public ScorePurchaseRequisitionResponse apply(ScorePurchaseRequisitionRequest input) {
                return procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scorePurchaseRequisition(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        return GetGoodsReceiptRatesResponse.newBuilder()
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getPurchaseRequisition().getBillingAddress().getPostalCode())
                                .build())
                        .setDueAt(reply0.getPurchaseRequisition().getDueAt())
                        .setGoodsReceiptId(request.getGoodsReceiptId())
                        .build())
                .build();
    }
}
