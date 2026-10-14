package com.acme.finance.domains.treasury;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.CountSanctionTotalsRequest;
import com.acme.compliance.v1.CountSanctionTotalsResponse;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.SubmitControlRequest;
import com.acme.compliance.v1.SubmitControlResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerCalculationRequest;
import com.acme.customers.v1.ExplainCustomerCalculationResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.customers.v1.RecalculateCustomersRequest;
import com.acme.customers.v1.RecalculateCustomersResponse;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.CountUsersRequest;
import com.acme.identity.v1.CountUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.PauseCollateralWorkflowRequest;
import com.acme.lending.v1.PauseCollateralWorkflowResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.ValidateMetricSeriesRequest;
import com.acme.metrics.v1.ValidateMetricSeriesResponse;
import com.acme.treasury.v1.ActivateFxHedgeRequest;
import com.acme.treasury.v1.ActivateFxHedgeResponse;
import com.acme.treasury.v1.CashPositionQueryServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CountCashPositionsRequest;
import com.acme.treasury.v1.CountCashPositionsResponse;
import com.acme.treasury.v1.ExpireFxHedgeRequest;
import com.acme.treasury.v1.ExpireFxHedgeResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.GetFxHedgeLifecycleRequest;
import com.acme.treasury.v1.GetFxHedgeLifecycleResponse;
import com.acme.treasury.v1.GetFxHedgeSyncStateRequest;
import com.acme.treasury.v1.GetFxHedgeSyncStateResponse;
import com.acme.treasury.v1.ImportLiquidityForecastsRequest;
import com.acme.treasury.v1.ImportLiquidityForecastsResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastSyncServiceGrpc;
import com.acme.treasury.v1.PushFxHedgeRequest;
import com.acme.treasury.v1.PushFxHedgeResponse;
import com.acme.treasury.v1.ReinstateFxHedgeRequest;
import com.acme.treasury.v1.ReinstateFxHedgeResponse;
import com.acme.treasury.v1.RenewFxHedgeRequest;
import com.acme.treasury.v1.RenewFxHedgeResponse;
import com.acme.treasury.v1.SuspendFxHedgeRequest;
import com.acme.treasury.v1.SuspendFxHedgeResponse;
import com.acme.treasury.v1.SyncLiquidityForecastsRequest;
import com.acme.treasury.v1.SyncLiquidityForecastsResponse;
import com.acme.treasury.v1.TerminateFxHedgeRequest;
import com.acme.treasury.v1.TerminateFxHedgeResponse;
import com.acme.venues.v1.SuggestVenuesRequest;
import com.acme.venues.v1.SuggestVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
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
 * Moves a fx hedge between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class FxHedgeLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(FxHedgeLifecycleApplication.class);

    private final ChannelRegistry channels;

    private final ControlApprovalServiceGrpc.ControlApprovalServiceBlockingStub complianceControlApprovalServiceBlocking;

    private final SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final CollateralWorkflowServiceGrpc.CollateralWorkflowServiceBlockingStub lendingCollateralWorkflowServiceBlocking;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final TreasuryRepository repository;

    private final Executor rpcExecutor;

    private final CashPositionQueryServiceGrpc.CashPositionQueryServiceBlockingStub treasuryCashPositionQueryServiceBlocking;

    private final FxHedgeSyncServiceGrpc.FxHedgeSyncServiceBlockingStub treasuryFxHedgeSyncServiceBlocking;

    private final LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceStub treasuryLiquidityForecastImportServiceAsync;

    private final LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceBlockingStub treasuryLiquidityForecastSyncServiceBlocking;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public FxHedgeLifecycleApplication(
            ChannelRegistry channels,
            ControlApprovalServiceGrpc.ControlApprovalServiceBlockingStub complianceControlApprovalServiceBlocking,
            SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            CollateralWorkflowServiceGrpc.CollateralWorkflowServiceBlockingStub lendingCollateralWorkflowServiceBlocking,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            TreasuryRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            CashPositionQueryServiceGrpc.CashPositionQueryServiceBlockingStub treasuryCashPositionQueryServiceBlocking,
            FxHedgeSyncServiceGrpc.FxHedgeSyncServiceBlockingStub treasuryFxHedgeSyncServiceBlocking,
            LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceStub treasuryLiquidityForecastImportServiceAsync,
            LiquidityForecastSyncServiceGrpc.LiquidityForecastSyncServiceBlockingStub treasuryLiquidityForecastSyncServiceBlocking,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.channels = channels;
        this.complianceControlApprovalServiceBlocking = complianceControlApprovalServiceBlocking;
        this.complianceSanctionReportingServiceBlocking = complianceSanctionReportingServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.lendingCollateralWorkflowServiceBlocking = lendingCollateralWorkflowServiceBlocking;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.treasuryCashPositionQueryServiceBlocking = treasuryCashPositionQueryServiceBlocking;
        this.treasuryFxHedgeSyncServiceBlocking = treasuryFxHedgeSyncServiceBlocking;
        this.treasuryLiquidityForecastImportServiceAsync = treasuryLiquidityForecastImportServiceAsync;
        this.treasuryLiquidityForecastSyncServiceBlocking = treasuryLiquidityForecastSyncServiceBlocking;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    /**
     * Activates a fx hedge.
     */
    public ActivateFxHedgeResponse activateFxHedge(ActivateFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "ActivateFxHedge");

        var outbound0 = ValidateMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateMetricSeriesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateMetricSeries(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = SyncLiquidityForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncLiquidityForecastsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getMetricSeries().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getMetricSeries().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getMetricSeries().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<SyncLiquidityForecastsRequest, SyncLiquidityForecastsResponse> port1 = RpcPort.adapt(treasuryLiquidityForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::syncLiquidityForecasts);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = RecalculateCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getLiquidityForecast().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setDetails(RecalculateCustomersRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply1.getLiquidityForecast().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        Function<RecalculateCustomersRequest, RecalculateCustomersResponse> operation2 = customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateCustomers;
        var reply2 = operation2.apply(outbound2);

        var outbound3 = PauseCollateralWorkflowRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getMetricSeries().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command3 = new DispatchCommand<>("acme.lending.v1.CollateralWorkflowService/PauseCollateralWorkflow", outbound3);
        var reply3 = lendingCollateralWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseCollateralWorkflow(command3.payload());
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateFxHedge", request.getDetails().getReasonCode());
        return ActivateFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getCustomer().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply3.getCollateral().getAddress().getLocality())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .setOutcome(ActivateFxHedgeResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Suspends a fx hedge.
     */
    public SuspendFxHedgeResponse suspendFxHedge(SuspendFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "SuspendFxHedge");

        var outbound0 = ImportLiquidityForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(ImportLiquidityForecastsRequest.StreamHeader.newBuilder()
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(ImportLiquidityForecastsResponse.getDefaultInstance());
        var input0 = treasuryLiquidityForecastImportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).importLiquidityForecasts(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();

        var outbound1 = CountCashPositionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountCashPositionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(treasuryCashPositionQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countCashPositions(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendFxHedge", request.getDetails().getReasonCode());
        return SuspendFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .setOutcome(SuspendFxHedgeResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Reinstates a fx hedge.
     */
    public ReinstateFxHedgeResponse reinstateFxHedge(ReinstateFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "ReinstateFxHedge");

        var outbound0 = SubmitControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .setDetails(SubmitControlRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<SubmitControlRequest, SubmitControlResponse> operation0 = complianceControlApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::submitControl;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = CountUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getControl().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getControl().getParametersMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countUsers(outbound1));
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateFxHedge", request.getDetails().getReasonCode());
        return ReinstateFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .setOutcome(ReinstateFxHedgeResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Terminates a fx hedge.
     */
    public TerminateFxHedgeResponse terminateFxHedge(TerminateFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "TerminateFxHedge");

        var outbound0 = CountSanctionTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSanctionTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> complianceSanctionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSanctionTotals(outbound0), rpcExecutor);

        var outbound1 = ScoreHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreHousehold(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateFxHedge", request.getDetails().getReasonCode());
        return TerminateFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getHousehold().getLocation().getRegion())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .setOutcome(TerminateFxHedgeResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Renews a fx hedge.
     */
    public RenewFxHedgeResponse renewFxHedge(RenewFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "RenewFxHedge");

        var outbound0 = ExplainCustomerCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerCalculation(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = RecalculateConsentRecordsRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateConsentRecordsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getCustomer().getPrimaryAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.customers.v1.ConsentRecordCalculationService");
        var selected1 = ConsentRecordCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.recalculateConsentRecords(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewFxHedge", request.getDetails().getReasonCode());
        return RenewFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getCustomer().getPrimaryAddress().getRecipient())
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(reply1.getConsentRecord().getSubject().getDisplayName())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .setOutcome(RenewFxHedgeResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Expires a fx hedge.
     */
    public ExpireFxHedgeResponse expireFxHedge(ExpireFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "ExpireFxHedge");

        var outbound0 = SuggestVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestVenues(outbound0), rpcExecutor);

        var outbound1 = PushFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushFxHedgeRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setFxHedge(FxHedge.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> treasuryFxHedgeSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushFxHedge(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireFxHedge", request.getDetails().getReasonCode());
        return ExpireFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(reply1.getFxHedge().getBillingAddress())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setStatus(reply1.getFxHedge().getStatus())
                        .build())
                .setOutcome(ExpireFxHedgeResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a fx hedge.
     */
    public GetFxHedgeLifecycleResponse getFxHedgeLifecycle(GetFxHedgeLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFxHedgeLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "GetFxHedgeLifecycle");

        var outbound0 = GetFxHedgeSyncStateRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedgeId(request.getFxHedgeId())
                .build();
        var channel0 = channels.lookup("acme.treasury.v1.FxHedgeSyncService");
        var selected0 = FxHedgeSyncServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.getFxHedgeSyncState(outbound0);
        return GetFxHedgeLifecycleResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getFxHedge().getAudit().getUpdatedBy())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .build();
    }
}
