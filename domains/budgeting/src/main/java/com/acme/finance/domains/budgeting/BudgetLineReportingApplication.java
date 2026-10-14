package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.CountBudgetLineTotalsRequest;
import com.acme.budgeting.v1.CountBudgetLineTotalsResponse;
import com.acme.budgeting.v1.DeleteBudgetLineReportRequest;
import com.acme.budgeting.v1.DeleteBudgetLineReportResponse;
import com.acme.budgeting.v1.ExplainForecastDecisionRequest;
import com.acme.budgeting.v1.ExplainForecastDecisionResponse;
import com.acme.budgeting.v1.ExplainVarianceDecisionRequest;
import com.acme.budgeting.v1.ExplainVarianceDecisionResponse;
import com.acme.budgeting.v1.ExportBudgetLinesRequest;
import com.acme.budgeting.v1.ExportBudgetLinesResponse;
import com.acme.budgeting.v1.FetchBudgetLineReportRequest;
import com.acme.budgeting.v1.FetchBudgetLineReportResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastCardSettlement;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.GenerateBudgetLineReportRequest;
import com.acme.budgeting.v1.GenerateBudgetLineReportResponse;
import com.acme.budgeting.v1.ListBudgetLineReportsRequest;
import com.acme.budgeting.v1.ListBudgetLineReportsResponse;
import com.acme.budgeting.v1.ListVarianceImportsRequest;
import com.acme.budgeting.v1.ListVarianceImportsResponse;
import com.acme.budgeting.v1.PushForecastRequest;
import com.acme.budgeting.v1.PushForecastResponse;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.budgeting.v1.ScheduleBudgetLineReportRequest;
import com.acme.budgeting.v1.ScheduleBudgetLineReportResponse;
import com.acme.budgeting.v1.ScoreVarianceRequest;
import com.acme.budgeting.v1.ScoreVarianceResponse;
import com.acme.budgeting.v1.ShareBudgetLineReportRequest;
import com.acme.budgeting.v1.ShareBudgetLineReportResponse;
import com.acme.budgeting.v1.StreamBudgetLineMetricsRequest;
import com.acme.budgeting.v1.StreamBudgetLineMetricsResponse;
import com.acme.budgeting.v1.ValidateForecastRequest;
import com.acme.budgeting.v1.ValidateForecastResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.BrowseUsersRequest;
import com.acme.identity.v1.BrowseUsersResponse;
import com.acme.identity.v1.CountUsersRequest;
import com.acme.identity.v1.CountUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.AdvancePolicyWorkflowRequest;
import com.acme.insurance.v1.AdvancePolicyWorkflowResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumCardSettlement;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.SearchLedgerAccountsRequest;
import com.acme.ledger.v1.SearchLedgerAccountsResponse;
import com.acme.risk.v1.AdvanceExposureWorkflowRequest;
import com.acme.risk.v1.AdvanceExposureWorkflowResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.CancelRiskScenarioWorkflowRequest;
import com.acme.risk.v1.CancelRiskScenarioWorkflowResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioStatusChange;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SummarizeSearchIndexesRequest;
import com.acme.search.v1.SummarizeSearchIndexesResponse;
import com.acme.suppliers.v1.PreviewSupplierRatingRequest;
import com.acme.suppliers.v1.PreviewSupplierRatingResponse;
import com.acme.suppliers.v1.ScoreSupplierRatingRequest;
import com.acme.suppliers.v1.ScoreSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about budget lines. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class BudgetLineReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetLineReportingApplication.class);

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final BudgetLineQueryServiceGrpc.BudgetLineQueryServiceBlockingStub budgetingBudgetLineQueryServiceBlocking;

    private final ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking;

    private final ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking;

    private final ForecastValidationServiceGrpc.ForecastValidationServiceBlockingStub budgetingForecastValidationServiceBlocking;

    private final VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking;

    private final VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking;

    private final BudgetingRepository repository;

    private final ExposureWorkflowServiceGrpc.ExposureWorkflowServiceBlockingStub riskExposureWorkflowServiceBlocking;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync;

    private final RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceBlockingStub riskRiskScenarioWorkflowServiceBlocking;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    public BudgetLineReportingApplication(
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            BudgetLineQueryServiceGrpc.BudgetLineQueryServiceBlockingStub budgetingBudgetLineQueryServiceBlocking,
            ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking,
            ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking,
            ForecastValidationServiceGrpc.ForecastValidationServiceBlockingStub budgetingForecastValidationServiceBlocking,
            VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking,
            VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking,
            ApplicationEventPublisher events,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceBlockingStub ledgerLedgerAccountServiceBlocking,
            BudgetingRepository repository,
            ExposureWorkflowServiceGrpc.ExposureWorkflowServiceBlockingStub riskExposureWorkflowServiceBlocking,
            RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync,
            RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceBlockingStub riskRiskScenarioWorkflowServiceBlocking,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking) {
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.budgetingBudgetLineQueryServiceBlocking = budgetingBudgetLineQueryServiceBlocking;
        this.budgetingForecastCalculationServiceBlocking = budgetingForecastCalculationServiceBlocking;
        this.budgetingForecastSyncServiceBlocking = budgetingForecastSyncServiceBlocking;
        this.budgetingForecastValidationServiceBlocking = budgetingForecastValidationServiceBlocking;
        this.budgetingVarianceImportServiceBlocking = budgetingVarianceImportServiceBlocking;
        this.budgetingVarianceValidationServiceBlocking = budgetingVarianceValidationServiceBlocking;
        this.events = events;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.insurancePolicyWorkflowServiceBlocking = insurancePolicyWorkflowServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.ledgerLedgerAccountServiceBlocking = ledgerLedgerAccountServiceBlocking;
        this.repository = repository;
        this.riskExposureWorkflowServiceBlocking = riskExposureWorkflowServiceBlocking;
        this.riskRiskRegisterServiceAsync = riskRiskRegisterServiceAsync;
        this.riskRiskScenarioWorkflowServiceBlocking = riskRiskScenarioWorkflowServiceBlocking;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
    }

    public record ListBudgetLineReportsStage0Requested(BrowseUsersRequest request, CompletableFuture<BrowseUsersResponse> completion, io.grpc.Context context) {}

    /**
     * Generates a budget line.
     */
    public GenerateBudgetLineReportResponse generateBudgetLineReport(GenerateBudgetLineReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateBudgetLineReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "GenerateBudgetLineReport");

        var outbound0 = SearchLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchLedgerAccountsRequest.Filter.newBuilder()
                        .putAllLabels(request.getDetails().getFiltersMap())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> ledgerLedgerAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchLedgerAccounts(value));

        var outbound1 = BatchCreateRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateRiskRegistersRequest.StreamHeader.newBuilder()
                        .setFormat(request.getDetails().getFormat())
                        .setSource(request.getBudgetLineId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateRiskRegistersResponse.getDefaultInstance());
        var input1 = riskRiskRegisterServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateRiskRegisters(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
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
        var collector2 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input2 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getCron()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateBudgetLineReport", request.getDetails().getCron());
        return GenerateBudgetLineReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a budget line.
     */
    public FetchBudgetLineReportResponse fetchBudgetLineReport(FetchBudgetLineReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchBudgetLineReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "FetchBudgetLineReport");

        var builder0 = PushForecastRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(PushForecastRequest.Details.newBuilder()
                .setPayload(JsonCodec.encode(request.getContext()))
                .build());
        builder0.setForecast(Forecast.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(request.getContext().getDeadline())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushForecast(outbound0);

        var outbound1 = QuoteForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteForecastRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply0.getForecast().getBillingAddress().getLocality())
                                .setRecipient(reply0.getForecast().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setCard(ForecastCardSettlement.newBuilder()
                                .setNetwork(reply0.getForecast().getCard().getNetwork())
                                .build())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setBudgetId(reply0.getForecast().getCounterparty().getBudgetId())
                                .build())
                        .build())
                .build();
        Function<QuoteForecastRequest, QuoteForecastResponse> operation1 = budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::quoteForecast;
        var reply1 = operation1.apply(outbound1);
        return FetchBudgetLineReportResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getForecast().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(reply0.getForecast().getBillingAddress())
                        .setBudgetLineId(request.getBudgetLineId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Schedules a budget line.
     */
    public ScheduleBudgetLineReportResponse scheduleBudgetLineReport(ScheduleBudgetLineReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleBudgetLineReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "ScheduleBudgetLineReport");

        var builder0 = ExplainForecastDecisionRequest.newBuilder();
        builder0.putAllAnnotations(request.getAnnotationsMap());
        builder0.setContext(request.getContext());
        builder0.setReason(request.getReason());
        var outbound0 = builder0.build();
        var reply0 = budgetingForecastValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainForecastDecision(outbound0);

        var outbound1 = AdvancePolicyWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getForecast().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(insurancePolicyWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advancePolicyWorkflow(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleBudgetLineReport", request.getBudgetLineId());
        return ScheduleBudgetLineReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams the changes of budget lines as they happen.
     */
    public StreamBudgetLineMetricsResponse streamBudgetLineMetrics(StreamBudgetLineMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamBudgetLineMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getContext().getTraceId(), "StreamBudgetLineMetrics");

        var outbound0 = ExportBudgetLinesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportBudgetLinesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setRegion(request.getFilter().getRegion())
                        .addAllStatuses(request.getFilter().getStatusesList())
                        .build())
                .build();
        var stream0 = budgetingBudgetLineQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportBudgetLines(outbound0);
        var reply0 = ExportBudgetLinesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = SummarizeSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SummarizeSearchIndexesRequest.Details.newBuilder()
                        .putAllFilters(request.getFilter().getLabelsMap())
                        .build())
                .build();
        Function<SummarizeSearchIndexesRequest, SummarizeSearchIndexesResponse> operation1 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::summarizeSearchIndexes;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = AdvanceExposureWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AdvanceExposureWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getSubscriber().getName())
                                .setPhone(request.getSubscriber().getPhone())
                                .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                .build())
                        .build())
                .setExposure(Exposure.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .build();
        var command2 = new DispatchCommand<>("acme.risk.v1.ExposureWorkflowService/AdvanceExposureWorkflow", outbound2);
        var reply2 = riskExposureWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceExposureWorkflow(command2.payload());

        var outbound3 = ResolvePremiumConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolvePremiumConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getExposure().getAudit().getCreatedBy())
                                .setUpdatedBy(reply2.getExposure().getAudit().getUpdatedBy())
                                .build())
                        .setCard(PremiumCardSettlement.newBuilder()
                                .setHolderName(switch (reply2.getExposure().getFundingCase()) {
                                    case CARD -> reply2.getExposure().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply2.getExposure().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolvePremiumConflict(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamBudgetLineMetrics", request.getFilter().getQuery());
        return StreamBudgetLineMetricsResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply3.getPremium().getBillingAddress().getCountryCode())
                                .setLocality(reply3.getPremium().getBillingAddress().getLocality())
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply2.getExposure().getContact().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Lists budget lines a page at a time.
     */
    public ListBudgetLineReportsResponse listBudgetLineReports(ListBudgetLineReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListBudgetLineReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getContext().getTraceId(), "ListBudgetLineReports");

        var outbound0 = BrowseUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseUsersRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setQuery(request.getFilter().getQuery())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var completion0 = new CompletableFuture<BrowseUsersResponse>();
        events.publishEvent(new ListBudgetLineReportsStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListBudgetLineReports", request.getPage().getPageToken());
        return ListBudgetLineReportsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply0.getPageInfo().getNextPageToken())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Shares a budget line.
     */
    public ShareBudgetLineReportResponse shareBudgetLineReport(ShareBudgetLineReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareBudgetLineReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "ShareBudgetLineReport");

        var outbound0 = ExplainVarianceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainVarianceDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getRecipient().getName())
                                .setPhone(request.getDetails().getRecipient().getPhone())
                                .build())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.budgeting.v1.VarianceValidationService/ExplainVarianceDecision", outbound0);
        var reply0 = budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainVarianceDecision(command0.payload());

        var outbound1 = RpcSupport.unpack(Any.pack(CountUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getVariance().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getVariance().getCustomFieldsMap())
                        .setOwnerId(reply0.getVariance().getSla().getPolicyId())
                        .build())
                .build()), CountUsersRequest.class);
        var reply1 = identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countUsers(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareBudgetLineReport", request.getDetails().getTemplateId());
        return ShareBudgetLineReportResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Counts budget lines by status.
     */
    public CountBudgetLineTotalsResponse countBudgetLineTotals(CountBudgetLineTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountBudgetLineTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getContext().getTraceId(), "CountBudgetLineTotals");

        var outbound0 = CancelRiskScenarioWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .putAllCustomFields(request.getFilter().getLabelsMap())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .addHistory(RiskScenarioStatusChange.newBuilder()
                                .setChangedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(riskRiskScenarioWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelRiskScenarioWorkflow(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = ScoreSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getRiskScenario().getCustomer().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreSupplierRating(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountBudgetLineTotals", request.getPage().getPageToken());
        return CountBudgetLineTotalsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Deletes a budget line.
     */
    public DeleteBudgetLineReportResponse deleteBudgetLineReport(DeleteBudgetLineReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteBudgetLineReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "DeleteBudgetLineReport");

        var outbound0 = PreviewSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewSupplierRatingRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setReason(request.getReason())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewSupplierRating(value));

        var outbound1 = ListVarianceImportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListVarianceImportsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getSupplierRating().getAudit().getUpdatedAt())
                        .setRegion(reply0.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        RpcPort<ListVarianceImportsRequest, ListVarianceImportsResponse> port1 = RpcPort.adapt(budgetingVarianceImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listVarianceImports);
        var reply1 = port1.exchange(outbound1);

        var builder2 = ScoreVarianceRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setDetails(ScoreVarianceRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .setPostalCode(reply0.getSupplierRating().getBillingAddress().getPostalCode())
                        .setRegion(reply0.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build());
        builder2.setVariance(Variance.newBuilder()
                .setRequester(ContactInfo.newBuilder()
                        .setEmail(reply0.getSupplierRating().getCounterparty().getEmail())
                        .build())
                .build());
        var outbound2 = builder2.build();
        var reply2 = budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreVariance(outbound2);

        var outbound3 = ValidateForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getSupplierRating().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var command3 = new DispatchCommand<>("acme.budgeting.v1.ForecastValidationService/ValidateForecast", outbound3);
        var reply3 = budgetingForecastValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateForecast(command3.payload());
        log.atDebug().addKeyValue("auditValue", request.getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteBudgetLineReport", request.getBudgetLineId());
        return DeleteBudgetLineReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }
}
