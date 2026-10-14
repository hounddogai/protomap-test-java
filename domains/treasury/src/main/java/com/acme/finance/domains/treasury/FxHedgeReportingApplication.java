package com.acme.finance.domains.treasury;

import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.StreamBudgetLineMetricsRequest;
import com.acme.budgeting.v1.StreamBudgetLineMetricsResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.EstimateHouseholdRequest;
import com.acme.customers.v1.EstimateHouseholdResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.WatchDocumentsRequest;
import com.acme.documents.v1.WatchDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.treasury.v1.CountFxHedgeTotalsRequest;
import com.acme.treasury.v1.CountFxHedgeTotalsResponse;
import com.acme.treasury.v1.DeleteFxHedgeReportRequest;
import com.acme.treasury.v1.DeleteFxHedgeReportResponse;
import com.acme.treasury.v1.FetchFxHedgeReportRequest;
import com.acme.treasury.v1.FetchFxHedgeReportResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.GenerateFxHedgeReportRequest;
import com.acme.treasury.v1.GenerateFxHedgeReportResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.ListFxHedgeReportsRequest;
import com.acme.treasury.v1.ListFxHedgeReportsResponse;
import com.acme.treasury.v1.ResolveCashPositionsRequest;
import com.acme.treasury.v1.ResolveCashPositionsResponse;
import com.acme.treasury.v1.StreamFxHedgeMetricsRequest;
import com.acme.treasury.v1.StreamFxHedgeMetricsResponse;
import com.acme.treasury.v1.ValidateLiquidityForecastImportRequest;
import com.acme.treasury.v1.ValidateLiquidityForecastImportResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about fx hedges. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class FxHedgeReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(FxHedgeReportingApplication.class);

    private final BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking;

    private final CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final ApplicationEventPublisher events;

    private final TreasuryRepository repository;

    private final LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking;

    public FxHedgeReportingApplication(
            BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking,
            CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            ApplicationEventPublisher events,
            TreasuryRepository repository,
            LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking) {
        this.budgetingBudgetLineReportingServiceBlocking = budgetingBudgetLineReportingServiceBlocking;
        this.collectionsCollectionCaseApprovalServiceBlocking = collectionsCollectionCaseApprovalServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.events = events;
        this.repository = repository;
        this.treasuryLiquidityForecastImportServiceBlocking = treasuryLiquidityForecastImportServiceBlocking;
    }

    public record DeleteFxHedgeReportStage0Requested(ResolveCashPositionsRequest request, CompletableFuture<ResolveCashPositionsResponse> completion, io.grpc.Context context) {}

    /**
     * Generates a fx hedge.
     */
    public GenerateFxHedgeReportResponse generateFxHedgeReport(GenerateFxHedgeReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateFxHedgeReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "GenerateFxHedgeReport");

        var outbound0 = RejectCollectionCaseRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RejectCollectionCaseRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        RpcPort<RejectCollectionCaseRequest, RejectCollectionCaseResponse> port0 = RpcPort.adapt(collectionsCollectionCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::rejectCollectionCase);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateFxHedgeReport", request.getFxHedgeId());
        return GenerateFxHedgeReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a fx hedge.
     */
    public FetchFxHedgeReportResponse fetchFxHedgeReport(FetchFxHedgeReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchFxHedgeReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "FetchFxHedgeReport");

        var outbound0 = ValidateLiquidityForecastImportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateLiquidityForecastImportRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ValidateLiquidityForecastImportRequest, ValidateLiquidityForecastImportResponse>() {
            @Override
            public ValidateLiquidityForecastImportResponse apply(ValidateLiquidityForecastImportRequest input) {
                return treasuryLiquidityForecastImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateLiquidityForecastImport(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        return FetchFxHedgeReportResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getLiquidityForecast().getBillingAddress().getLinesList())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setTotal(reply0.getLiquidityForecast().getTotal())
                        .build())
                .build();
    }

    /**
     * Streams the changes of fx hedges as they happen.
     */
    public StreamFxHedgeMetricsResponse streamFxHedgeMetrics(StreamFxHedgeMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamFxHedgeMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getContext().getTraceId(), "StreamFxHedgeMetrics");

        var outbound0 = StreamBudgetLineMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamBudgetLineMetricsRequest.Filter.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var stream0 = budgetingBudgetLineReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamBudgetLineMetrics(outbound0);
        var reply0 = StreamBudgetLineMetricsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamFxHedgeMetrics", request.getFilter().getQuery());
        return StreamFxHedgeMetricsResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists fx hedges a page at a time.
     */
    public ListFxHedgeReportsResponse listFxHedgeReports(ListFxHedgeReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListFxHedgeReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getContext().getTraceId(), "ListFxHedgeReports");

        var outbound0 = WatchDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchDocumentsRequest.Filter.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var stream0 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchDocuments(outbound0);
        var reply0 = WatchDocumentsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListFxHedgeReports", request.getPage().getPageToken());
        return ListFxHedgeReportsResponse.getDefaultInstance();
    }

    /**
     * Counts fx hedges by status.
     */
    public CountFxHedgeTotalsResponse countFxHedgeTotals(CountFxHedgeTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountFxHedgeTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getContext().getTraceId(), "CountFxHedgeTotals");

        var outbound0 = BeanCopies.copy(EstimateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .build())
                                .setLocation(PostalAddress.newBuilder()
                                        .setRegion(request.getFilter().getRegion())
                                        .build())
                                .build())
                        .build())
                .build(), EstimateHouseholdRequest.class);
        var reply0 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateHousehold(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountFxHedgeTotals", request.getPage().getPageToken());
        return CountFxHedgeTotalsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Deletes a fx hedge.
     */
    public DeleteFxHedgeReportResponse deleteFxHedgeReport(DeleteFxHedgeReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteFxHedgeReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "DeleteFxHedgeReport");

        var outbound0 = ResolveCashPositionsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<ResolveCashPositionsResponse>();
        events.publishEvent(new DeleteFxHedgeReportStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteFxHedgeReport", request.getFxHedgeId());
        return DeleteFxHedgeReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
