package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.GenerateDisposalReportRequest;
import com.acme.assets.v1.GenerateDisposalReportResponse;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.budgeting.v1.AdvanceForecastWorkflowRequest;
import com.acme.budgeting.v1.AdvanceForecastWorkflowResponse;
import com.acme.budgeting.v1.ApproveForecastRequest;
import com.acme.budgeting.v1.ApproveForecastResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLineRef;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.CancelForecastWorkflowRequest;
import com.acme.budgeting.v1.CancelForecastWorkflowResponse;
import com.acme.budgeting.v1.CountBudgetLineTotalsRequest;
import com.acme.budgeting.v1.CountBudgetLineTotalsResponse;
import com.acme.budgeting.v1.EscalateBudgetRequest;
import com.acme.budgeting.v1.EscalateBudgetResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastApprovalServiceGrpc;
import com.acme.budgeting.v1.ForecastCardSettlement;
import com.acme.budgeting.v1.ForecastLine;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.GetForecastSyncStateRequest;
import com.acme.budgeting.v1.GetForecastSyncStateResponse;
import com.acme.budgeting.v1.InspectForecastWorkflowRequest;
import com.acme.budgeting.v1.InspectForecastWorkflowResponse;
import com.acme.budgeting.v1.ListForecastWorkflowsRequest;
import com.acme.budgeting.v1.ListForecastWorkflowsResponse;
import com.acme.budgeting.v1.PauseForecastWorkflowRequest;
import com.acme.budgeting.v1.PauseForecastWorkflowResponse;
import com.acme.budgeting.v1.PeekBudgetRequest;
import com.acme.budgeting.v1.PeekBudgetResponse;
import com.acme.budgeting.v1.ResolveVarianceConflictRequest;
import com.acme.budgeting.v1.ResolveVarianceConflictResponse;
import com.acme.budgeting.v1.ResumeForecastWorkflowRequest;
import com.acme.budgeting.v1.ResumeForecastWorkflowResponse;
import com.acme.budgeting.v1.RetryForecastWorkflowRequest;
import com.acme.budgeting.v1.RetryForecastWorkflowResponse;
import com.acme.budgeting.v1.ShareForecastReportRequest;
import com.acme.budgeting.v1.ShareForecastReportResponse;
import com.acme.budgeting.v1.StartForecastWorkflowRequest;
import com.acme.budgeting.v1.StartForecastWorkflowResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.collections.v1.ApprovePaymentPromiseRequest;
import com.acme.collections.v1.ApprovePaymentPromiseResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityRequest;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.NormalizeFeatureFlagRequest;
import com.acme.featureflags.v1.NormalizeFeatureFlagResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageLifecycleServiceGrpc;
import com.acme.insurance.v1.ScheduleCoverageExpiryRequest;
import com.acme.insurance.v1.ScheduleCoverageExpiryResponse;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityResponse;
import com.acme.suppliers.v1.CountSuppliersRequest;
import com.acme.suppliers.v1.CountSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
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
 * Runs the long-lived workflow that takes a forecast from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ForecastWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(ForecastWorkflowApplication.class);

    private final DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking;

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking;

    private final BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking;

    private final ForecastApprovalServiceGrpc.ForecastApprovalServiceBlockingStub budgetingForecastApprovalServiceBlocking;

    private final ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceBlockingStub budgetingVarianceSyncServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking;

    private final PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceFutureStub collectionsPaymentPromiseApprovalServiceFuture;

    private final ApplicationEventPublisher events;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final CoverageLifecycleServiceGrpc.CoverageLifecycleServiceBlockingStub insuranceCoverageLifecycleServiceBlocking;

    private final BudgetingRepository repository;

    private final Executor rpcExecutor;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    public ForecastWorkflowApplication(
            DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking,
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking,
            BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking,
            ForecastApprovalServiceGrpc.ForecastApprovalServiceBlockingStub budgetingForecastApprovalServiceBlocking,
            ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking,
            VarianceSyncServiceGrpc.VarianceSyncServiceBlockingStub budgetingVarianceSyncServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseApprovalServiceGrpc.CollectionCaseApprovalServiceBlockingStub collectionsCollectionCaseApprovalServiceBlocking,
            PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceFutureStub collectionsPaymentPromiseApprovalServiceFuture,
            ApplicationEventPublisher events,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            CoverageLifecycleServiceGrpc.CoverageLifecycleServiceBlockingStub insuranceCoverageLifecycleServiceBlocking,
            BudgetingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking) {
        this.assetsDisposalReportingServiceBlocking = assetsDisposalReportingServiceBlocking;
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.budgetingBudgetLineReportingServiceBlocking = budgetingBudgetLineReportingServiceBlocking;
        this.budgetingBudgetQueryServiceBlocking = budgetingBudgetQueryServiceBlocking;
        this.budgetingForecastApprovalServiceBlocking = budgetingForecastApprovalServiceBlocking;
        this.budgetingForecastReportingServiceBlocking = budgetingForecastReportingServiceBlocking;
        this.budgetingVarianceSyncServiceBlocking = budgetingVarianceSyncServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseApprovalServiceBlocking = collectionsCollectionCaseApprovalServiceBlocking;
        this.collectionsPaymentPromiseApprovalServiceFuture = collectionsPaymentPromiseApprovalServiceFuture;
        this.events = events;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.insuranceCoverageLifecycleServiceBlocking = insuranceCoverageLifecycleServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
    }

    public record CancelForecastWorkflowStage1Requested(EscalateBudgetRequest request, CompletableFuture<EscalateBudgetResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a forecast.
     */
    public StartForecastWorkflowResponse startForecastWorkflow(StartForecastWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartForecastWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = CountSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getForecast().getAudit().getCreatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(request.getForecast().getForecastId())
                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSuppliers(outbound0), rpcExecutor);

        var outbound1 = PeekBudgetRequest.newBuilder()
                .setBudgetId(request.getForecast().getCounterparty().getBudgetId())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> budgetingBudgetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).peekBudget(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getForecast().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartForecastWorkflow", request.getForecast().getNumber());
        return StartForecastWorkflowResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setDueAt(request.getForecast().getDueAt())
                        .setForecastId(request.getForecastId())
                        .build())
                .setOutcome(StartForecastWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances a forecast.
     */
    public AdvanceForecastWorkflowResponse advanceForecastWorkflow(AdvanceForecastWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceForecastWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        List<GenerateDisposalReportResponse> results0 = request.getForecast().getLinesList().stream()
                .map(element -> GenerateDisposalReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(GenerateDisposalReportRequest.Details.newBuilder()
                        .putAllFilters(element.getAttributesMap())
                        .build())
                .setReason(request.getReason())
                .build())
                .flatMap(value -> Stream.of(assetsDisposalReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateDisposalReport(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? GenerateDisposalReportResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ResolveVarianceConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveVarianceConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getForecast().getAudit().getUpdatedAt())
                                .build())
                        .setCustomer(BudgetLineRef.newBuilder()
                                .setDisplayName(request.getForecast().getCounterparty().getDisplayName())
                                .setNumber(request.getForecast().getNumber())
                                .build())
                        .build())
                .build();
        RpcPort<ResolveVarianceConflictRequest, ResolveVarianceConflictResponse> port1 = RpcPort.adapt(budgetingVarianceSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::resolveVarianceConflict);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceForecastWorkflow", request.getForecast().getForecastId());
        return AdvanceForecastWorkflowResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .setIssuedAt(request.getForecast().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a forecast.
     */
    public CancelForecastWorkflowResponse cancelForecastWorkflow(CancelForecastWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelForecastWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = (ShareRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareRetentionPolicyExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareRetentionPolicyExport(outbound0);

        var outbound1 = EscalateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getForecast().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getForecast().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EscalateBudgetRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(reply0.getRetentionPolicy().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<EscalateBudgetResponse>();
        events.publishEvent(new CancelForecastWorkflowStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = CheckSupplierRatingEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckSupplierRatingEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply1.getBudget().getContact().getPhone())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getForecast().getBillingAddress().getLocality())
                                .build())
                        .setCard(SupplierRatingCardSettlement.newBuilder()
                                .setToken(switch (request.getForecast().getSettlementCase()) {
                                    case CARD -> request.getForecast().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getRetentionPolicy().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkSupplierRatingEligibility(outbound2);

        var outbound3 = JsonCodec.roundTrip(CheckFeatureFlagEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRetentionPolicy().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getRetentionPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply2.getSupplierRating().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build(), CheckFeatureFlagEligibilityRequest.class);
        var reply3 = featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkFeatureFlagEligibility(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelForecastWorkflow", request.getForecast().getForecastId());
        return CancelForecastWorkflowResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply2.getSupplierRating().getBillingAddress().getCountryCode())
                                .build())
                        .setForecastId(request.getForecastId())
                        .addLines(ForecastLine.newBuilder()
                                .setAmount(reply3.getFeatureFlag().getDiscount().getAmount())
                                .build())
                        .putAllMetadata(request.getForecast().getMetadataMap())
                        .build())
                .setOutcome(CancelForecastWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setMessage(reply2.getOutcome().getMessage())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a forecast.
     */
    public RetryForecastWorkflowResponse retryForecastWorkflow(RetryForecastWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryForecastWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = ShareForecastReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareForecastReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = budgetingForecastReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareForecastReport(outbound0);

        var outbound1 = ScheduleCoverageExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getForecast().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setDetails(ScheduleCoverageExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        Function<ScheduleCoverageExpiryRequest, ScheduleCoverageExpiryResponse> operation1 = insuranceCoverageLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scheduleCoverageExpiry;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryForecastWorkflow", request.getForecast().getForecastId());
        return RetryForecastWorkflowResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getForecast().getBillingAddress().getLocality())
                                .build())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setDisplayName(reply1.getCoverage().getOwner().getDisplayName())
                                .build())
                        .setForecastId(request.getForecastId())
                        .setNumber(request.getForecast().getNumber())
                        .build())
                .build();
    }

    /**
     * Pauses a forecast.
     */
    public PauseForecastWorkflowResponse pauseForecastWorkflow(PauseForecastWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseForecastWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = ApprovePaymentPromiseRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getNotify().getName())
                                        .setPhone(request.getDetails().getNotify().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending0 = collectionsPaymentPromiseApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).approvePaymentPromise(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseForecastWorkflow", request.getForecast().getForecastId());
        return PauseForecastWorkflowResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .setNotes(request.getForecast().getNotes())
                        .build())
                .setOutcome(PauseForecastWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Resumes a forecast.
     */
    public ResumeForecastWorkflowResponse resumeForecastWorkflow(ResumeForecastWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeForecastWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = ApproveForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getForecast().getBillingAddress().getLocality())
                                .build())
                        .setCard(ForecastCardSettlement.newBuilder()
                                .setHolderName(switch (request.getForecast().getSettlementCase()) {
                                    case CARD -> request.getForecast().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = budgetingForecastApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveForecast(outbound0);

        var outbound1 = RejectCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getForecast().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage1Adapter {
            RejectCollectionCaseResponse fetch(RejectCollectionCaseRequest input) {
                return collectionsCollectionCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectCollectionCase(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeForecastWorkflow", request.getForecast().getForecastId());
        return ResumeForecastWorkflowResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getCollectionCase().getAudit().getUpdatedAt())
                                .build())
                        .setForecastId(request.getForecastId())
                        .setTotal(request.getForecast().getTotal())
                        .build())
                .setOutcome(ResumeForecastWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a forecast.
     */
    public InspectForecastWorkflowResponse inspectForecastWorkflow(InspectForecastWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectForecastWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "InspectForecastWorkflow");

        var outbound0 = NormalizeFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeFeatureFlagRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        RpcPort<NormalizeFeatureFlagRequest, NormalizeFeatureFlagResponse> port0 = RpcPort.adapt(featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeFeatureFlag);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = GetForecastSyncStateRequest.newBuilder()
                .setContext(request.getContext())
                .setForecastId(request.getForecastId())
                .build();
        var channel1 = channels.lookup("acme.budgeting.v1.ForecastSyncService");
        var selected1 = ForecastSyncServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.getForecastSyncState(outbound1);
        return InspectForecastWorkflowResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getForecast().getAudit().getEtag())
                                .build())
                        .setForecastId(request.getForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists forecasts a page at a time.
     */
    public ListForecastWorkflowsResponse listForecastWorkflows(ListForecastWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListForecastWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getContext().getTraceId(), "ListForecastWorkflows");

        var outbound0 = (CountBudgetLineTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountBudgetLineTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .build())
                .build()).toBuilder().build();
        var reply0 = budgetingBudgetLineReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countBudgetLineTotals(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListForecastWorkflows", request.getPage().getPageToken());
        return ListForecastWorkflowsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
