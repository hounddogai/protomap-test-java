package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.FixedAssetWorkflowServiceGrpc;
import com.acme.assets.v1.ReconcileAssetTransfersRequest;
import com.acme.assets.v1.ReconcileAssetTransfersResponse;
import com.acme.assets.v1.SignalFixedAssetWorkflowRequest;
import com.acme.assets.v1.SignalFixedAssetWorkflowResponse;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.budgeting.v1.ActivateBudgetRequest;
import com.acme.budgeting.v1.ActivateBudgetResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.ExpireBudgetRequest;
import com.acme.budgeting.v1.ExpireBudgetResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.GetBudgetLifecycleRequest;
import com.acme.budgeting.v1.GetBudgetLifecycleResponse;
import com.acme.budgeting.v1.GetBudgetRatesRequest;
import com.acme.budgeting.v1.GetBudgetRatesResponse;
import com.acme.budgeting.v1.NormalizeBudgetLineRequest;
import com.acme.budgeting.v1.NormalizeBudgetLineResponse;
import com.acme.budgeting.v1.PullForecastsRequest;
import com.acme.budgeting.v1.PullForecastsResponse;
import com.acme.budgeting.v1.ReinstateBudgetRequest;
import com.acme.budgeting.v1.ReinstateBudgetResponse;
import com.acme.budgeting.v1.RenewBudgetRequest;
import com.acme.budgeting.v1.RenewBudgetResponse;
import com.acme.budgeting.v1.ScheduleBudgetExpiryRequest;
import com.acme.budgeting.v1.ScheduleBudgetExpiryResponse;
import com.acme.budgeting.v1.ScoreBudgetLineRequest;
import com.acme.budgeting.v1.ScoreBudgetLineResponse;
import com.acme.budgeting.v1.SuspendBudgetRequest;
import com.acme.budgeting.v1.SuspendBudgetResponse;
import com.acme.budgeting.v1.TerminateBudgetRequest;
import com.acme.budgeting.v1.TerminateBudgetResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.CloneFeatureFlagRequest;
import com.acme.featureflags.v1.CloneFeatureFlagResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import com.acme.suppliers.v1.WatchSuppliersRequest;
import com.acme.suppliers.v1.WatchSuppliersResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Moves a budget between the states of its lifecycle. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class BudgetLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetLifecycleApplication.class);

    private final AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking;

    private final FixedAssetWorkflowServiceGrpc.FixedAssetWorkflowServiceFutureStub assetsFixedAssetWorkflowServiceFuture;

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking;

    private final ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking;

    private final ChannelRegistry channels;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking;

    private final BudgetLifecycleRequestMapper mapper;

    private final BudgetingRepository repository;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public BudgetLifecycleApplication(
            AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking,
            FixedAssetWorkflowServiceGrpc.FixedAssetWorkflowServiceFutureStub assetsFixedAssetWorkflowServiceFuture,
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking,
            ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking,
            ChannelRegistry channels,
            FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking,
            BudgetLifecycleRequestMapper mapper,
            BudgetingRepository repository,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.assetsAssetTransferSyncServiceBlocking = assetsAssetTransferSyncServiceBlocking;
        this.assetsFixedAssetWorkflowServiceFuture = assetsFixedAssetWorkflowServiceFuture;
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.budgetingBudgetLineValidationServiceBlocking = budgetingBudgetLineValidationServiceBlocking;
        this.budgetingForecastSyncServiceBlocking = budgetingForecastSyncServiceBlocking;
        this.channels = channels;
        this.featureflagsFeatureFlagServiceBlocking = featureflagsFeatureFlagServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    /**
     * Activates a budget.
     */
    public ActivateBudgetResponse activateBudget(ActivateBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "ActivateBudget");

        var outbound0 = BeanCopies.copy(CloneFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build(), CloneFeatureFlagRequest.class);
        var reply0 = featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneFeatureFlag(outbound0);

        var outbound1 = SubscribeAuditEventUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeAuditEventUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getFeatureFlag().getOwner().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setReason(switch (reply0.getFeatureFlag().getActionCase()) {
                    case BLOCK -> reply0.getFeatureFlag().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply1 = auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeAuditEventUpdates(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateBudget", request.getDetails().getReasonCode());
        return ActivateBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getFeatureFlag().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getFeatureFlag().getAudit().getCreatedBy())
                                .build())
                        .setBudgetId(request.getBudgetId())
                        .build())
                .setOutcome(ActivateBudgetResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Suspends a budget.
     */
    public SuspendBudgetResponse suspendBudget(SuspendBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "SuspendBudget");

        var outbound0 = NormalizeBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeBudgetLineRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
        RpcPort<NormalizeBudgetLineRequest, NormalizeBudgetLineResponse> port0 = RpcPort.adapt(budgetingBudgetLineValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeBudgetLine);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendBudget", request.getDetails().getReasonCode());
        return SuspendBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .build())
                .setOutcome(SuspendBudgetResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Reinstates a budget.
     */
    public ReinstateBudgetResponse reinstateBudget(ReinstateBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "ReinstateBudget");

        var outbound0 = (ReconcileAssetTransfersRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ReconcileAssetTransfersRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build()).toBuilder().build();
        var reply0 = assetsAssetTransferSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileAssetTransfers(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateBudget", request.getDetails().getReasonCode());
        return ReinstateBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .build())
                .setOutcome(ReinstateBudgetResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Terminates a budget.
     */
    public TerminateBudgetResponse terminateBudget(TerminateBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "TerminateBudget");

        var outbound0 = SignalFixedAssetWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SignalFixedAssetWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
        var pending0 = assetsFixedAssetWorkflowServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).signalFixedAssetWorkflow(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = ValidateSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateSupplierRatingRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getFixedAsset().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getFixedAsset().getAddress().getLinesList())
                                .setLocality(switch (reply0.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> reply0.getFixedAsset().getAddress().getLocality();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSupplierRating(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateBudget", request.getDetails().getReasonCode());
        return TerminateBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getSupplierRating().getAudit().getCreatedBy())
                                .setUpdatedBy(reply1.getSupplierRating().getAudit().getUpdatedBy())
                                .build())
                        .setBudgetId(request.getBudgetId())
                        .build())
                .setOutcome(TerminateBudgetResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Renews a budget.
     */
    public RenewBudgetResponse renewBudget(RenewBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "RenewBudget");

        var outbound0 = WatchSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNewOwner().getEmail())
                        .setName(request.getDetails().getNotifyContact().getName())
                        .build())
                .build();
        var stream0 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchSuppliers(outbound0);
        var reply0 = WatchSuppliersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = mapper.renewBudgetStage1(request);
        var reply1 = auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeAuditEventUpdates(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewBudget", request.getDetails().getReasonCode());
        return RenewBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .build())
                .setOutcome(RenewBudgetResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Expires a budget.
     */
    public ExpireBudgetResponse expireBudget(ExpireBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "ExpireBudget");

        var outbound0 = PullForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullForecastsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setCounterparty(BudgetRef.newBuilder()
                                .setBudgetId(request.getBudgetId())
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<PullForecastsRequest, PullForecastsResponse> operation0 = budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::pullForecasts;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireBudget", request.getDetails().getReasonCode());
        return ExpireBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getForecast().getAudit().getCreatedBy())
                                .build())
                        .setBudgetId(request.getBudgetId())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .setOutcome(ExpireBudgetResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Returns a budget.
     */
    public GetBudgetLifecycleResponse getBudgetLifecycle(GetBudgetLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetBudgetLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "GetBudgetLifecycle");

        var outbound0 = ScoreBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            ScoreBudgetLineResponse fetch(ScoreBudgetLineRequest input) {
                return budgetingBudgetLineValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreBudgetLine(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        return GetBudgetLifecycleResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setHolder(ForecastRef.newBuilder()
                                .setNumber(reply0.getBudgetLine().getNumber())
                                .build())
                        .build())
                .build();
    }

    /**
     * Schedules a budget.
     */
    public ScheduleBudgetExpiryResponse scheduleBudgetExpiry(ScheduleBudgetExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleBudgetExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = GetBudgetRatesRequest.newBuilder()
                .setBudgetId(request.getBudgetId())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.budgeting.v1.BudgetCalculationService");
        var serviceName0 = "acme.budgeting.v1" + "." + "BudgetCalculationService";
        var methodName0 = "Get" + "BudgetRates";
        var descriptor0 = MethodDescriptor.<GetBudgetRatesRequest, GetBudgetRatesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(GetBudgetRatesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(GetBudgetRatesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleBudgetExpiry", request.getBudget().getBudgetId());
        return ScheduleBudgetExpiryResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setContact(request.getBudget().getContact())
                        .putAllLabels(reply0.getBudget().getLabelsMap())
                        .build())
                .build();
    }
}
