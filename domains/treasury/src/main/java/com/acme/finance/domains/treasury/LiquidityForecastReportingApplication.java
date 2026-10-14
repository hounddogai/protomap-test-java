package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineApprovalServiceGrpc;
import com.acme.budgeting.v1.EscalateBudgetLineRequest;
import com.acme.budgeting.v1.EscalateBudgetLineResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.ExplainConsentRecordDecisionRequest;
import com.acme.customers.v1.ExplainConsentRecordDecisionResponse;
import com.acme.customers.v1.ExplainHouseholdDecisionRequest;
import com.acme.customers.v1.ExplainHouseholdDecisionResponse;
import com.acme.customers.v1.FindCustomersRequest;
import com.acme.customers.v1.FindCustomersResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.ScoreCustomerRequest;
import com.acme.customers.v1.ScoreCustomerResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunBankSettlement;
import com.acme.payroll.v1.PayRunWorkflowServiceGrpc;
import com.acme.payroll.v1.RetryPayRunWorkflowRequest;
import com.acme.payroll.v1.RetryPayRunWorkflowResponse;
import com.acme.performance.v1.CreateReviewRequest;
import com.acme.performance.v1.CreateReviewResponse;
import com.acme.performance.v1.Review;
import com.acme.performance.v1.ReviewLine;
import com.acme.performance.v1.ReviewServiceGrpc;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderLifecycleServiceGrpc;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryRequest;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.acme.treasury.v1.CountLiquidityForecastTotalsRequest;
import com.acme.treasury.v1.CountLiquidityForecastTotalsResponse;
import com.acme.treasury.v1.DeleteLiquidityForecastReportRequest;
import com.acme.treasury.v1.DeleteLiquidityForecastReportResponse;
import com.acme.treasury.v1.FetchLiquidityForecastReportRequest;
import com.acme.treasury.v1.FetchLiquidityForecastReportResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeReportingServiceGrpc;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.GenerateFxHedgeReportRequest;
import com.acme.treasury.v1.GenerateFxHedgeReportResponse;
import com.acme.treasury.v1.GenerateLiquidityForecastReportRequest;
import com.acme.treasury.v1.GenerateLiquidityForecastReportResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCardSettlement;
import com.acme.treasury.v1.LiquidityForecastValidationServiceGrpc;
import com.acme.treasury.v1.ScheduleLiquidityForecastReportRequest;
import com.acme.treasury.v1.ScheduleLiquidityForecastReportResponse;
import com.acme.treasury.v1.ScoreLiquidityForecastRequest;
import com.acme.treasury.v1.ScoreLiquidityForecastResponse;
import com.acme.treasury.v1.ShareLiquidityForecastReportRequest;
import com.acme.treasury.v1.ShareLiquidityForecastReportResponse;
import com.acme.treasury.v1.StreamLiquidityForecastMetricsRequest;
import com.acme.treasury.v1.StreamLiquidityForecastMetricsResponse;
import com.acme.treasury.v1.SyncFxHedgesRequest;
import com.acme.treasury.v1.SyncFxHedgesResponse;
import com.acme.treasury.v1.UpdateCashPositionRequest;
import com.acme.treasury.v1.UpdateCashPositionResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about liquidity forecasts. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class LiquidityForecastReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(LiquidityForecastReportingApplication.class);

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final BudgetLineApprovalServiceGrpc.BudgetLineApprovalServiceBlockingStub budgetingBudgetLineApprovalServiceBlocking;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final LiquidityForecastReportingRequestMapper mapper;

    private final PayRunWorkflowServiceGrpc.PayRunWorkflowServiceBlockingStub payrollPayRunWorkflowServiceBlocking;

    private final ReviewServiceGrpc.ReviewServiceBlockingStub performanceReviewServiceBlocking;

    private final PurchaseOrderLifecycleServiceGrpc.PurchaseOrderLifecycleServiceBlockingStub procurementPurchaseOrderLifecycleServiceBlocking;

    private final TreasuryRepository repository;

    private final CashPositionServiceGrpc.CashPositionServiceBlockingStub treasuryCashPositionServiceBlocking;

    private final FxHedgeReportingServiceGrpc.FxHedgeReportingServiceBlockingStub treasuryFxHedgeReportingServiceBlocking;

    private final FxHedgeSyncServiceGrpc.FxHedgeSyncServiceBlockingStub treasuryFxHedgeSyncServiceBlocking;

    private final LiquidityForecastValidationServiceGrpc.LiquidityForecastValidationServiceBlockingStub treasuryLiquidityForecastValidationServiceBlocking;

    public LiquidityForecastReportingApplication(
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            BudgetLineApprovalServiceGrpc.BudgetLineApprovalServiceBlockingStub budgetingBudgetLineApprovalServiceBlocking,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            LiquidityForecastReportingRequestMapper mapper,
            PayRunWorkflowServiceGrpc.PayRunWorkflowServiceBlockingStub payrollPayRunWorkflowServiceBlocking,
            ReviewServiceGrpc.ReviewServiceBlockingStub performanceReviewServiceBlocking,
            PurchaseOrderLifecycleServiceGrpc.PurchaseOrderLifecycleServiceBlockingStub procurementPurchaseOrderLifecycleServiceBlocking,
            TreasuryRepository repository,
            CashPositionServiceGrpc.CashPositionServiceBlockingStub treasuryCashPositionServiceBlocking,
            FxHedgeReportingServiceGrpc.FxHedgeReportingServiceBlockingStub treasuryFxHedgeReportingServiceBlocking,
            FxHedgeSyncServiceGrpc.FxHedgeSyncServiceBlockingStub treasuryFxHedgeSyncServiceBlocking,
            LiquidityForecastValidationServiceGrpc.LiquidityForecastValidationServiceBlockingStub treasuryLiquidityForecastValidationServiceBlocking) {
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.budgetingBudgetLineApprovalServiceBlocking = budgetingBudgetLineApprovalServiceBlocking;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.mapper = mapper;
        this.payrollPayRunWorkflowServiceBlocking = payrollPayRunWorkflowServiceBlocking;
        this.performanceReviewServiceBlocking = performanceReviewServiceBlocking;
        this.procurementPurchaseOrderLifecycleServiceBlocking = procurementPurchaseOrderLifecycleServiceBlocking;
        this.repository = repository;
        this.treasuryCashPositionServiceBlocking = treasuryCashPositionServiceBlocking;
        this.treasuryFxHedgeReportingServiceBlocking = treasuryFxHedgeReportingServiceBlocking;
        this.treasuryFxHedgeSyncServiceBlocking = treasuryFxHedgeSyncServiceBlocking;
        this.treasuryLiquidityForecastValidationServiceBlocking = treasuryLiquidityForecastValidationServiceBlocking;
    }

    /**
     * Generates a liquidity forecast.
     */
    public GenerateLiquidityForecastReportResponse generateLiquidityForecastReport(GenerateLiquidityForecastReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateLiquidityForecastReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "GenerateLiquidityForecastReport");

        var outbound0 = RpcSupport.unpack(Any.pack(SyncFxHedgesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SyncFxHedgesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .putAllMetadata(request.getDetails().getFiltersMap())
                        .build())
                .setReason(request.getReason())
                .build()), SyncFxHedgesRequest.class);
        var reply0 = treasuryFxHedgeSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncFxHedges(outbound0);

        var outbound1 = ScoreLiquidityForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getFxHedge().getAudit().getUpdatedAt())
                                .build())
                        .setCard(LiquidityForecastCardSettlement.newBuilder()
                                .setToken(switch (reply0.getFxHedge().getSettlementCase()) {
                                    case CARD -> reply0.getFxHedge().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = treasuryLiquidityForecastValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreLiquidityForecast(outbound1);

        var outbound2 = RetryPayRunWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setBank(PayRunBankSettlement.newBuilder()
                                .setIban(switch (reply0.getFxHedge().getSettlementCase()) {
                                    case BANK -> reply0.getFxHedge().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getLiquidityForecast().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply2 = payrollPayRunWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryPayRunWorkflow(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateLiquidityForecastReport", request.getLiquidityForecastId());
        return GenerateLiquidityForecastReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a liquidity forecast.
     */
    public FetchLiquidityForecastReportResponse fetchLiquidityForecastReport(FetchLiquidityForecastReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchLiquidityForecastReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "FetchLiquidityForecastReport");

        var outbound0 = ExplainConsentRecordDecisionRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainConsentRecordDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainConsentRecordDecision(value));
        return FetchLiquidityForecastReportResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Schedules a liquidity forecast.
     */
    public ScheduleLiquidityForecastReportResponse scheduleLiquidityForecastReport(ScheduleLiquidityForecastReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleLiquidityForecastReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "ScheduleLiquidityForecastReport");

        var builder0 = GenerateFxHedgeReportRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(GenerateFxHedgeReportRequest.Details.newBuilder()
                .setCron(request.getDetails().getCron())
                .putAllFilters(request.getDetails().getFiltersMap())
                .setFormat(request.getDetails().getFormat())
                .build());
        var outbound0 = builder0.build();
        var reply0 = treasuryFxHedgeReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateFxHedgeReport(outbound0);

        var outbound1 = CompareHouseholdOptionsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getContext().getActor().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        RpcPort<CompareHouseholdOptionsRequest, CompareHouseholdOptionsResponse> port1 = RpcPort.adapt(customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::compareHouseholdOptions);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = FindCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindCustomersRequest.Filter.newBuilder()
                        .setRegion(reply1.getHousehold().getLocation().getRegion())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findCustomers(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleLiquidityForecastReport", request.getLiquidityForecastId());
        return ScheduleLiquidityForecastReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams the changes of liquidity forecasts as they happen.
     */
    public StreamLiquidityForecastMetricsResponse streamLiquidityForecastMetrics(StreamLiquidityForecastMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamLiquidityForecastMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getContext().getTraceId(), "StreamLiquidityForecastMetrics");

        var builder0 = UpdateCashPositionRequest.newBuilder();
        builder0.setCashPosition(CashPosition.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getSubscriber().getEmail())
                        .setPhone(request.getSubscriber().getPhone())
                        .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                        .build())
                .build());
        builder0.setContext(request.getContext());
        var outbound0 = builder0.build();
        var reply0 = treasuryCashPositionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateCashPosition(outbound0);

        var outbound1 = ExplainHouseholdDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply0.getCashPosition().getAudit().getCreatedAt())
                                                .setUpdatedAt(reply0.getCashPosition().getAudit().getUpdatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply0.getCashPosition().getContact().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainHouseholdDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamLiquidityForecastMetrics", request.getFilter().getQuery());
        return StreamLiquidityForecastMetricsResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCashPosition().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getHousehold().getLocation().getPostalCode())
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Shares a liquidity forecast.
     */
    public ShareLiquidityForecastReportResponse shareLiquidityForecastReport(ShareLiquidityForecastReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareLiquidityForecastReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "ShareLiquidityForecastReport");

        var outbound0 = mapper.shareLiquidityForecastReportStage0(request);
        var reply0 = performanceReviewServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createReview(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(SchedulePurchaseOrderExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getReview().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build()), SchedulePurchaseOrderExpiryRequest.class);
        var reply1 = procurementPurchaseOrderLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).schedulePurchaseOrderExpiry(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getEmail()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareLiquidityForecastReport", request.getDetails().getRecipient().getEmail());
        return ShareLiquidityForecastReportResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setNumber(reply1.getPurchaseOrder().getCounterparty().getNumber())
                        .build())
                .setOutcome(ShareLiquidityForecastReportResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Counts liquidity forecasts by status.
     */
    public CountLiquidityForecastTotalsResponse countLiquidityForecastTotals(CountLiquidityForecastTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountLiquidityForecastTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getContext().getTraceId(), "CountLiquidityForecastTotals");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountLiquidityForecastTotals", request.getPage().getPageToken());
        return CountLiquidityForecastTotalsResponse.getDefaultInstance();
    }

    /**
     * Deletes a liquidity forecast.
     */
    public DeleteLiquidityForecastReportResponse deleteLiquidityForecastReport(DeleteLiquidityForecastReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteLiquidityForecastReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "DeleteLiquidityForecastReport");

        var outbound0 = EscalateBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = budgetingBudgetLineApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateBudgetLine(outbound0);

        var outbound1 = ScoreCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getBudgetLine().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<ScoreCustomerRequest, ScoreCustomerResponse> port1 = RpcPort.adapt(customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scoreCustomer);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = PreviewAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getBudgetLine().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getCustomer().getPrimaryAddress().getCountryCode())
                                .build())
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setNetwork(switch (reply0.getBudgetLine().getSettlementCase()) {
                                    case CARD -> reply0.getBudgetLine().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAuditTrailRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply1.getCustomer().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply2 = auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAuditTrail(outbound2);

        var outbound3 = ExplainConsentRecordDecisionRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(reply0.getBudgetLine().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainConsentRecordDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getBudgetLine().getCounterparty().getEmail())
                                .setName(reply1.getCustomer().getName())
                                .setPhone(reply1.getCustomer().getContact().getPhone())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainConsentRecordDecisionRequest, ExplainConsentRecordDecisionResponse> port3 = RpcPort.adapt(customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainConsentRecordDecision);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteLiquidityForecastReport", request.getLiquidityForecastId());
        return DeleteLiquidityForecastReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }
}
