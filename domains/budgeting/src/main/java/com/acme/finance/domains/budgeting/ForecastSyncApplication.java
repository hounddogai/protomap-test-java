package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BrowseBudgetsRequest;
import com.acme.budgeting.v1.BrowseBudgetsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineAttachment;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineRef;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.CalculateBudgetLineRequest;
import com.acme.budgeting.v1.CalculateBudgetLineResponse;
import com.acme.budgeting.v1.CompareBudgetOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetOptionsResponse;
import com.acme.budgeting.v1.CompareForecastOptionsRequest;
import com.acme.budgeting.v1.CompareForecastOptionsResponse;
import com.acme.budgeting.v1.EstimateBudgetLineRequest;
import com.acme.budgeting.v1.EstimateBudgetLineResponse;
import com.acme.budgeting.v1.ExplainForecastCalculationRequest;
import com.acme.budgeting.v1.ExplainForecastCalculationResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastBankSettlement;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.GetForecastSyncStateRequest;
import com.acme.budgeting.v1.GetForecastSyncStateResponse;
import com.acme.budgeting.v1.ListForecastConflictsRequest;
import com.acme.budgeting.v1.ListForecastConflictsResponse;
import com.acme.budgeting.v1.PullForecastsRequest;
import com.acme.budgeting.v1.PullForecastsResponse;
import com.acme.budgeting.v1.PushForecastRequest;
import com.acme.budgeting.v1.PushForecastResponse;
import com.acme.budgeting.v1.ReconcileForecastsRequest;
import com.acme.budgeting.v1.ReconcileForecastsResponse;
import com.acme.budgeting.v1.ResolveForecastConflictRequest;
import com.acme.budgeting.v1.ResolveForecastConflictResponse;
import com.acme.budgeting.v1.RetryVarianceImportRequest;
import com.acme.budgeting.v1.RetryVarianceImportResponse;
import com.acme.budgeting.v1.StreamForecastChangesRequest;
import com.acme.budgeting.v1.StreamForecastChangesResponse;
import com.acme.budgeting.v1.SyncForecastsRequest;
import com.acme.budgeting.v1.SyncForecastsResponse;
import com.acme.budgeting.v1.ValidateForecastRequest;
import com.acme.budgeting.v1.ValidateForecastResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.Forum;
import com.acme.community.v1.ForumServiceGrpc;
import com.acme.community.v1.UpdateForumRequest;
import com.acme.community.v1.UpdateForumResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseWorkflowServiceGrpc;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListUnderwritingConflictsRequest;
import com.acme.insurance.v1.ListUnderwritingConflictsResponse;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.keymanagement.v1.CryptoKeyServiceGrpc;
import com.acme.keymanagement.v1.WatchCryptoKeysRequest;
import com.acme.keymanagement.v1.WatchCryptoKeysResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.StreamFiscalPeriodExportRequest;
import com.acme.ledger.v1.StreamFiscalPeriodExportResponse;
import com.acme.orders.v1.EstimateOrderRequest;
import com.acme.orders.v1.EstimateOrderResponse;
import com.acme.orders.v1.Order;
import com.acme.orders.v1.OrderCalculationServiceGrpc;
import com.acme.risk.v1.ApproveRiskScenarioRequest;
import com.acme.risk.v1.ApproveRiskScenarioResponse;
import com.acme.risk.v1.RejectRiskRegisterRequest;
import com.acme.risk.v1.RejectRiskRegisterResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.SubmitRiskRegisterRequest;
import com.acme.risk.v1.SubmitRiskRegisterResponse;
import com.acme.suppliers.v1.FindSuppliersRequest;
import com.acme.suppliers.v1.FindSuppliersResponse;
import com.acme.suppliers.v1.ListSuppliersRequest;
import com.acme.suppliers.v1.ListSuppliersResponse;
import com.acme.suppliers.v1.PreviewSupplierRatingRequest;
import com.acme.suppliers.v1.PreviewSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.venues.v1.CloneVenueRequest;
import com.acme.venues.v1.CloneVenueResponse;
import com.acme.venues.v1.Venue;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Keeps forecasts in step with the systems of record in other regions. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ForecastSyncApplication {
    private static final Logger log = LoggerFactory.getLogger(ForecastSyncApplication.class);

    private final BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking;

    private final BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking;

    private final ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking;

    private final ForecastCalculationServiceGrpc.ForecastCalculationServiceFutureStub budgetingForecastCalculationServiceFuture;

    private final ForecastValidationServiceGrpc.ForecastValidationServiceFutureStub budgetingForecastValidationServiceFuture;

    private final VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking;

    private final ChannelRegistry channels;

    private final ForumServiceGrpc.ForumServiceBlockingStub communityForumServiceBlocking;

    private final ComplianceCaseWorkflowServiceGrpc.ComplianceCaseWorkflowServiceBlockingStub complianceComplianceCaseWorkflowServiceBlocking;

    private final ApplicationEventPublisher events;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking;

    private final CryptoKeyServiceGrpc.CryptoKeyServiceBlockingStub keymanagementCryptoKeyServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final OrderCalculationServiceGrpc.OrderCalculationServiceBlockingStub ordersOrderCalculationServiceBlocking;

    private final BudgetingRepository repository;

    private final RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking;

    private final RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public ForecastSyncApplication(
            BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking,
            BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking,
            BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking,
            ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking,
            ForecastCalculationServiceGrpc.ForecastCalculationServiceFutureStub budgetingForecastCalculationServiceFuture,
            ForecastValidationServiceGrpc.ForecastValidationServiceFutureStub budgetingForecastValidationServiceFuture,
            VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking,
            ChannelRegistry channels,
            ForumServiceGrpc.ForumServiceBlockingStub communityForumServiceBlocking,
            ComplianceCaseWorkflowServiceGrpc.ComplianceCaseWorkflowServiceBlockingStub complianceComplianceCaseWorkflowServiceBlocking,
            ApplicationEventPublisher events,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking,
            CryptoKeyServiceGrpc.CryptoKeyServiceBlockingStub keymanagementCryptoKeyServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            OrderCalculationServiceGrpc.OrderCalculationServiceBlockingStub ordersOrderCalculationServiceBlocking,
            BudgetingRepository repository,
            RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking,
            RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.budgetingBudgetCalculationServiceBlocking = budgetingBudgetCalculationServiceBlocking;
        this.budgetingBudgetLineCalculationServiceBlocking = budgetingBudgetLineCalculationServiceBlocking;
        this.budgetingBudgetQueryServiceBlocking = budgetingBudgetQueryServiceBlocking;
        this.budgetingForecastCalculationServiceBlocking = budgetingForecastCalculationServiceBlocking;
        this.budgetingForecastCalculationServiceFuture = budgetingForecastCalculationServiceFuture;
        this.budgetingForecastValidationServiceFuture = budgetingForecastValidationServiceFuture;
        this.budgetingVarianceImportServiceBlocking = budgetingVarianceImportServiceBlocking;
        this.channels = channels;
        this.communityForumServiceBlocking = communityForumServiceBlocking;
        this.complianceComplianceCaseWorkflowServiceBlocking = complianceComplianceCaseWorkflowServiceBlocking;
        this.events = events;
        this.insuranceUnderwritingSyncServiceBlocking = insuranceUnderwritingSyncServiceBlocking;
        this.keymanagementCryptoKeyServiceBlocking = keymanagementCryptoKeyServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.ordersOrderCalculationServiceBlocking = ordersOrderCalculationServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterApprovalServiceBlocking = riskRiskRegisterApprovalServiceBlocking;
        this.riskRiskScenarioApprovalServiceBlocking = riskRiskScenarioApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    public record ResolveForecastConflictStage1Requested(FindSuppliersRequest request, CompletableFuture<FindSuppliersResponse> completion, io.grpc.Context context) {}

    /**
     * Syncs a forecast.
     */
    public SyncForecastsResponse syncForecasts(SyncForecastsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SyncForecasts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = CalculateBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBank(BudgetLineBankSettlement.newBuilder()
                                .setIban(switch (request.getForecast().getSettlementCase()) {
                                    case BANK -> request.getForecast().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getForecast().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateBudgetLine(outbound0);

        var outbound1 = ListUnderwritingConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListUnderwritingConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getForecast().getAudit().getUpdatedAt())
                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var adapter1 = new Function<ListUnderwritingConflictsRequest, ListUnderwritingConflictsResponse>() {
            @Override
            public ListUnderwritingConflictsResponse apply(ListUnderwritingConflictsRequest input) {
                return insuranceUnderwritingSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listUnderwritingConflicts(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SyncForecasts", request.getForecast().getForecastId());
        return SyncForecastsResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setCounterparty(request.getForecast().getCounterparty())
                        .setForecastId(request.getForecastId())
                        .build())
                .setOutcome(SyncForecastsResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Pushs a forecast.
     */
    public PushForecastResponse pushForecast(PushForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PushForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = SubmitRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getForecast().getAudit().getCreatedAt())
                                                .setCreatedBy(request.getForecast().getAudit().getCreatedBy())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setLocality(request.getForecast().getBillingAddress().getLocality())
                                                .build())
                                        .build())
                                .setLocation(PostalAddress.newBuilder()
                                        .setPostalCode(request.getForecast().getBillingAddress().getPostalCode())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.risk.v1.RiskRegisterApprovalService");
        var selected0 = RiskRegisterApprovalServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.submitRiskRegister(outbound0);

        var outbound1 = CompareBudgetOptionsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setCard(BudgetCardFunding.newBuilder()
                                .setHolderName(switch (request.getForecast().getSettlementCase()) {
                                    case CARD -> request.getForecast().getCard().getHolderName();
                                    default -> "";
                                })
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
        var reply1 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareBudgetOptions(outbound1);

        var outbound2 = PreviewSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getBudget().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply1.getBudget().getHolder().getDisplayName())
                                .setEmail(reply1.getBudget().getContact().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<PreviewSupplierRatingRequest, PreviewSupplierRatingResponse> port2 = RpcPort.adapt(suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewSupplierRating);
        var reply2 = port2.exchange(outbound2);

        var outbound3 = RpcSupport.unpack(Any.pack(CompareForecastOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getBudget().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply2.getSupplierRating().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build()), CompareForecastOptionsRequest.class);
        var reply3 = budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareForecastOptions(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PushForecast", request.getForecast().getForecastId());
        return PushForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRiskRegister().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getBudget().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply3.getForecast().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setCurrencyCode(reply3.getBreakdown().getCurrencyCode())
                                .build())
                        .setForecastId(request.getForecastId())
                        .setTotal(request.getForecast().getTotal())
                        .build())
                .setOutcome(PushForecastResponse.Outcome.newBuilder()
                        .setReferenceId(reply2.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Pulls a forecast.
     */
    public PullForecastsResponse pullForecasts(PullForecastsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PullForecasts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        List<ListSuppliersResponse> results0 = request.getForecast().getLinesList().stream()
                .map(element -> ListSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getForecast().getAudit().getCreatedAt())
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                        .build())
                .build())
                .flatMap(value -> Stream.of(suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listSuppliers(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ListSuppliersResponse.getDefaultInstance()
                : results0.getLast();

        var builder1 = RejectRiskRegisterRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setRiskRegister(RiskRegister.newBuilder()
                .addChildren(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setCreatedAt(request.getForecast().getAudit().getCreatedAt())
                                        .build())
                                .setLocation(PostalAddress.newBuilder()
                                        .setLocality(request.getForecast().getBillingAddress().getLocality())
                                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                                        .build())
                                .build())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = riskRiskRegisterApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectRiskRegister(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PullForecasts", request.getForecast().getForecastId());
        return PullForecastsResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setDueAt(request.getForecast().getDueAt())
                        .setForecastId(request.getForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Reconciles a forecast.
     */
    public ReconcileForecastsResponse reconcileForecasts(ReconcileForecastsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReconcileForecasts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = BrowseBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseBudgetsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getForecast().getAudit().getCreatedAt())
                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> budgetingBudgetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseBudgets(outbound0));

        var outbound1 = RetryVarianceImportRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getForecast().getAudit().getCreatedAt())
                                .build())
                        .setCustomer(BudgetLineRef.newBuilder()
                                .setNumber(request.getForecast().getNumber())
                                .build())
                        .build())
                .build();
        var reply1 = budgetingVarianceImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryVarianceImport(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReconcileForecasts", request.getForecast().getForecastId());
        return ReconcileForecastsResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .setIssuedAt(request.getForecast().getIssuedAt())
                        .build())
                .setOutcome(ReconcileForecastsResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Exchanges changes of forecasts with a peer.
     */
    public StreamForecastChangesResponse streamForecastChanges(StreamForecastChangesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamForecastChanges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasChange()) {
            repository.saveForecast(request.getContext().getTenantId(), request.getChange());
        }

        var futures0 = request.getChange().getAttachmentsList().stream()
                .map(element -> EstimateBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .addAttachments(BudgetLineAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setFileName(element.getFileName())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateBudgetLine(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? EstimateBudgetLineResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = StreamFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamFiscalPeriodExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getChange().getDueAt())
                        .setOwnerId(request.getChange().getForecastId())
                        .setRegion(request.getChange().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamFiscalPeriodExport(outbound1);
        var reply1 = StreamFiscalPeriodExportResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = WatchCryptoKeysRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchCryptoKeysRequest.Filter.newBuilder()
                        .setRegion(reply0.getBudgetLine().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream2 = keymanagementCryptoKeyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchCryptoKeys(outbound2);
        var reply2 = WatchCryptoKeysResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = ApproveRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(switch (request.getFrameCase()) {
                                    case CHANGE -> request.getChange().getCounterparty().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply3 = riskRiskScenarioApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveRiskScenario(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getChange().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamForecastChanges", request.getChange().getNumber());
        return StreamForecastChangesResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setCounterparty(BudgetRef.newBuilder()
                                .setDisplayName(reply3.getRiskScenario().getCustomer().getDisplayName())
                                .build())
                        .setNotes(request.getChange().getNotes())
                        .setStatus(request.getChange().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Resolves a forecast.
     */
    public ResolveForecastConflictResponse resolveForecastConflict(ResolveForecastConflictRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveForecastConflict")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = ExplainForecastCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setBank(ForecastBankSettlement.newBuilder()
                                .setIban(request.getForecast().getBank().getIban())
                                .build())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setBudgetId(request.getForecast().getCounterparty().getBudgetId())
                                .setDisplayName(request.getForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = budgetingForecastCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).explainForecastCalculation(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = FindSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getForecast().getAudit().getCreatedAt())
                        .putAllLabels(request.getForecast().getMetadataMap())
                        .setOwnerId(reply0.getForecast().getCounterparty().getBudgetId())
                        .setRegion(reply0.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion1 = new CompletableFuture<FindSuppliersResponse>();
        events.publishEvent(new ResolveForecastConflictStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = CloneVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getForecast().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        RpcPort<CloneVenueRequest, CloneVenueResponse> port2 = RpcPort.adapt(venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::cloneVenue);
        var reply2 = port2.exchange(outbound2);

        var outbound3 = EstimateOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setOrder(Order.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply2.getVenue().getAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply3 = ordersOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateOrder(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResolveForecastConflict", request.getForecast().getForecastId());
        return ResolveForecastConflictResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(request.getForecast().getBillingAddress())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setCurrencyCode(reply0.getForecast().getCounterparty().getCurrencyCode())
                                .setDisplayName(reply3.getOrder().getCounterparty().getDisplayName())
                                .build())
                        .setForecastId(request.getForecastId())
                        .build())
                .setOutcome(ResolveForecastConflictResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a forecast.
     */
    public GetForecastSyncStateResponse getForecastSyncState(GetForecastSyncStateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetForecastSyncState")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "GetForecastSyncState");

        var outbound0 = ValidateForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateForecastRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .build())
                .build();
        var pending0 = budgetingForecastValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).validateForecast(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = UpdateForumRequest.newBuilder()
                .setContext(request.getContext())
                .setForum(Forum.newBuilder()
                        .addChildren(Forum.newBuilder()
                                .addChildren(Forum.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply0.getForecast().getAudit().getCreatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply0.getForecast().getCounterparty().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = communityForumServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateForum(outbound1);
        return GetForecastSyncStateResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getForum().getAudit().getUpdatedAt())
                                .build())
                        .setForecastId(request.getForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists forecasts a page at a time.
     */
    public ListForecastConflictsResponse listForecastConflicts(ListForecastConflictsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListForecastConflicts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getContext().getTraceId(), "ListForecastConflicts");

        var builder0 = ResumeComplianceCaseWorkflowRequest.newBuilder();
        builder0.setComplianceCase(ComplianceCase.newBuilder()
                .setRequester(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build());
        builder0.setContext(request.getContext());
        var outbound0 = builder0.build();
        var reply0 = complianceComplianceCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeComplianceCaseWorkflow(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListForecastConflicts", request.getPage().getPageToken());
        return ListForecastConflictsResponse.newBuilder()
                .addForecasts(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getComplianceCase().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getComplianceCase().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
    }
}
