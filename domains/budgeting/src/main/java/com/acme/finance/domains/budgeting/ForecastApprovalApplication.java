package com.acme.finance.domains.budgeting;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.LintAuditTrailsRequest;
import com.acme.audit.v1.LintAuditTrailsResponse;
import com.acme.audit.v1.PullRetentionPoliciesRequest;
import com.acme.audit.v1.PullRetentionPoliciesResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.budgeting.v1.AdvanceBudgetWorkflowRequest;
import com.acme.budgeting.v1.AdvanceBudgetWorkflowResponse;
import com.acme.budgeting.v1.ApproveForecastRequest;
import com.acme.budgeting.v1.ApproveForecastResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineCardSettlement;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetWorkflowServiceGrpc;
import com.acme.budgeting.v1.CancelBudgetWorkflowRequest;
import com.acme.budgeting.v1.CancelBudgetWorkflowResponse;
import com.acme.budgeting.v1.CommentBudgetLineApprovalRequest;
import com.acme.budgeting.v1.CommentBudgetLineApprovalResponse;
import com.acme.budgeting.v1.CommentForecastApprovalRequest;
import com.acme.budgeting.v1.CommentForecastApprovalResponse;
import com.acme.budgeting.v1.CompareBudgetLineOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetLineOptionsResponse;
import com.acme.budgeting.v1.DelegateForecastApprovalRequest;
import com.acme.budgeting.v1.DelegateForecastApprovalResponse;
import com.acme.budgeting.v1.EscalateBudgetRequest;
import com.acme.budgeting.v1.EscalateBudgetResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.GetForecastApprovalStatusRequest;
import com.acme.budgeting.v1.GetForecastApprovalStatusResponse;
import com.acme.budgeting.v1.GetForecastRatesRequest;
import com.acme.budgeting.v1.GetForecastRatesResponse;
import com.acme.budgeting.v1.ListForecastReportsRequest;
import com.acme.budgeting.v1.ListForecastReportsResponse;
import com.acme.budgeting.v1.ListPendingForecastsRequest;
import com.acme.budgeting.v1.ListPendingForecastsResponse;
import com.acme.budgeting.v1.NormalizeBudgetRequest;
import com.acme.budgeting.v1.NormalizeBudgetResponse;
import com.acme.budgeting.v1.RecallForecastRequest;
import com.acme.budgeting.v1.RecallForecastResponse;
import com.acme.budgeting.v1.RejectForecastRequest;
import com.acme.budgeting.v1.RejectForecastResponse;
import com.acme.budgeting.v1.SubmitForecastRequest;
import com.acme.budgeting.v1.SubmitForecastResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentApprovalServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.SubmitRepaymentRequest;
import com.acme.lending.v1.SubmitRepaymentResponse;
import com.acme.metrics.v1.ExplainMetricSeriesDecisionRequest;
import com.acme.metrics.v1.ExplainMetricSeriesDecisionResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.ResolveSuppliersRequest;
import com.acme.suppliers.v1.ResolveSuppliersResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingLine;
import com.acme.suppliers.v1.SupplierRatingLineTax;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import com.acme.suppliers.v1.WatchSuppliersRequest;
import com.acme.suppliers.v1.WatchSuppliersResponse;
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
 * Routes forecasts through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ForecastApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(ForecastApprovalApplication.class);

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final BudgetApprovalServiceGrpc.BudgetApprovalServiceBlockingStub budgetingBudgetApprovalServiceBlocking;

    private final BudgetLineApprovalServiceGrpc.BudgetLineApprovalServiceBlockingStub budgetingBudgetLineApprovalServiceBlocking;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking;

    private final BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking;

    private final BudgetWorkflowServiceGrpc.BudgetWorkflowServiceBlockingStub budgetingBudgetWorkflowServiceBlocking;

    private final ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking;

    private final ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking;

    private final RepaymentApprovalServiceGrpc.RepaymentApprovalServiceBlockingStub lendingRepaymentApprovalServiceBlocking;

    private final ForecastApprovalRequestMapper mapper;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final BudgetingRepository repository;

    private final Executor rpcExecutor;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public ForecastApprovalApplication(
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            BudgetApprovalServiceGrpc.BudgetApprovalServiceBlockingStub budgetingBudgetApprovalServiceBlocking,
            BudgetLineApprovalServiceGrpc.BudgetLineApprovalServiceBlockingStub budgetingBudgetLineApprovalServiceBlocking,
            BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking,
            BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking,
            BudgetWorkflowServiceGrpc.BudgetWorkflowServiceBlockingStub budgetingBudgetWorkflowServiceBlocking,
            ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking,
            ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking,
            RepaymentApprovalServiceGrpc.RepaymentApprovalServiceBlockingStub lendingRepaymentApprovalServiceBlocking,
            ForecastApprovalRequestMapper mapper,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            BudgetingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.budgetingBudgetApprovalServiceBlocking = budgetingBudgetApprovalServiceBlocking;
        this.budgetingBudgetLineApprovalServiceBlocking = budgetingBudgetLineApprovalServiceBlocking;
        this.budgetingBudgetLineCalculationServiceBlocking = budgetingBudgetLineCalculationServiceBlocking;
        this.budgetingBudgetValidationServiceBlocking = budgetingBudgetValidationServiceBlocking;
        this.budgetingBudgetWorkflowServiceBlocking = budgetingBudgetWorkflowServiceBlocking;
        this.budgetingForecastCalculationServiceBlocking = budgetingForecastCalculationServiceBlocking;
        this.budgetingForecastReportingServiceBlocking = budgetingForecastReportingServiceBlocking;
        this.lendingRepaymentApprovalServiceBlocking = lendingRepaymentApprovalServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    /**
     * Submits a forecast.
     */
    public SubmitForecastResponse submitForecast(SubmitForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = CreateSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplier(Supplier.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getForecast().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        RpcPort<CreateSupplierRequest, CreateSupplierResponse> port0 = RpcPort.adapt(suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createSupplier);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = mapper.submitForecastStage1(request);
        var reply1 = budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getForecastRates(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitForecast", request.getForecast().getNumber());
        return SubmitForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getSupplier().getPrimaryAddress().getCountryCode())
                                .build())
                        .setForecastId(request.getForecastId())
                        .setNumber(reply0.getSupplier().getPassport().getNumber())
                        .setTotal(request.getForecast().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Approves a forecast.
     */
    public ApproveForecastResponse approveForecast(ApproveForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = ListForecastReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListForecastReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.budgeting.v1.ForecastReportingService/ListForecastReports", outbound0);
        var reply0 = budgetingForecastReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listForecastReports(command0.payload());

        var outbound1 = SubmitRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getApprover().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(lendingRepaymentApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitRepayment(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveForecast", request.getForecast().getForecastId());
        return ApproveForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getRepayment().getLocation().getCountryCode())
                                .build())
                        .setForecastId(request.getForecastId())
                        .build())
                .setOutcome(ApproveForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Rejects a forecast.
     */
    public RejectForecastResponse rejectForecast(RejectForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = EscalateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EscalateBudgetRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<EscalateBudgetRequest, EscalateBudgetResponse>() {
            @Override
            public EscalateBudgetResponse apply(EscalateBudgetRequest input) {
                return budgetingBudgetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateBudget(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = CompareBudgetLineOptionsRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getForecast().getAudit().getCreatedBy())
                                .build())
                        .setBank(BudgetLineBankSettlement.newBuilder()
                                .setBic(switch (request.getForecast().getSettlementCase()) {
                                    case BANK -> request.getForecast().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareBudgetLineOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getForecast().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply1 = budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareBudgetLineOptions(outbound1);

        var outbound2 = CommentBudgetLineApprovalRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setCard(BudgetLineCardSettlement.newBuilder()
                                .setLast4(reply0.getBudget().getCard().getLast4())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CommentBudgetLineApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(budgetingBudgetLineApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentBudgetLineApproval(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = LintAuditTrailsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getBudgetLine().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter3 = new Function<LintAuditTrailsRequest, LintAuditTrailsResponse>() {
            @Override
            public LintAuditTrailsResponse apply(LintAuditTrailsRequest input) {
                return auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAuditTrails(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectForecast", request.getForecast().getForecastId());
        return RejectForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getBudgetLine().getAudit().getCreatedBy())
                                .build())
                        .setForecastId(request.getForecastId())
                        .setIssuedAt(reply3.getAuditTrail().getIssuedAt())
                        .setNotes(reply2.getBudgetLine().getNotes())
                        .setNumber(request.getForecast().getNumber())
                        .build())
                .setOutcome(RejectForecastResponse.Outcome.newBuilder()
                        .setMessage(reply3.getOutcome().getMessage())
                        .setReferenceId(reply2.getOutcome().getReferenceId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists forecasts a page at a time.
     */
    public ListPendingForecastsResponse listPendingForecasts(ListPendingForecastsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingForecasts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getContext().getTraceId(), "ListPendingForecasts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingForecasts", request.getPage().getPageToken());
        return ListPendingForecastsResponse.getDefaultInstance();
    }

    /**
     * Delegates a forecast.
     */
    public DelegateForecastApprovalResponse delegateForecastApproval(DelegateForecastApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateForecastApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = BeanCopies.copy(AdvanceBudgetWorkflowRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(request.getForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), AdvanceBudgetWorkflowRequest.class);
        var reply0 = budgetingBudgetWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceBudgetWorkflow(outbound0);

        var futures1 = request.getForecast().getAttachmentsList().stream()
                .map(element -> PullRetentionPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullRetentionPoliciesRequest.Details.newBuilder()
                        .setChecksum(element.getChecksum())
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getBudget().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullRetentionPolicies(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? PullRetentionPoliciesResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = WatchSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchSuppliersRequest.Filter.newBuilder()
                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream2 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchSuppliers(outbound2);
        var reply2 = WatchSuppliersResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateForecastApproval", request.getForecast().getForecastId());
        return DelegateForecastApprovalResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .setNumber(request.getForecast().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Recalls a forecast.
     */
    public RecallForecastResponse recallForecast(RecallForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = CancelBudgetWorkflowRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setCard(BudgetCardFunding.newBuilder()
                                .setNetwork(request.getForecast().getCard().getNetwork())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> budgetingBudgetWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelBudgetWorkflow(value));

        var outbound1 = mapper.recallForecastStage1(request);
        var reply1 = metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainMetricSeriesDecision(outbound1);

        var futures2 = request.getForecast().getLinesList().stream()
                .map(element -> ValidateSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .addLines(SupplierRatingLine.newBuilder()
                                .setTax(SupplierRatingLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSupplierRating(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? ValidateSupplierRatingResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallForecast", request.getForecast().getForecastId());
        return RecallForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .setStatus(request.getForecast().getStatus())
                        .build())
                .setOutcome(RecallForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a forecast.
     */
    public GetForecastApprovalStatusResponse getForecastApprovalStatus(GetForecastApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetForecastApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "GetForecastApprovalStatus");

        var outbound0 = BeanCopies.copy(ListForecastReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListForecastReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getForecastId())
                        .build())
                .build(), ListForecastReportsRequest.class);
        var reply0 = budgetingForecastReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listForecastReports(outbound0);
        return GetForecastApprovalStatusResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Comments a forecast.
     */
    public CommentForecastApprovalResponse commentForecastApproval(CommentForecastApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentForecastApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = NormalizeBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setCard(BudgetCardFunding.newBuilder()
                                .setLast4(switch (request.getForecast().getSettlementCase()) {
                                    case CARD -> request.getForecast().getCard().getLast4();
                                    default -> "";
                                })
                                .setToken(switch (request.getForecast().getSettlementCase()) {
                                    case CARD -> request.getForecast().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeBudget(outbound0);

        var outbound1 = BeanCopies.copy(ResolveSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .build(), ResolveSuppliersRequest.class);
        var reply1 = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveSuppliers(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentForecastApproval", request.getForecast().getForecastId());
        return CommentForecastApprovalResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .putAllMetadata(request.getForecast().getMetadataMap())
                        .build())
                .setOutcome(CommentForecastApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }
}
