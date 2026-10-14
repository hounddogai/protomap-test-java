package com.acme.finance.domains.assets;

import com.acme.assets.v1.AdvanceFixedAssetWorkflowRequest;
import com.acme.assets.v1.AdvanceFixedAssetWorkflowResponse;
import com.acme.assets.v1.ApproveDepreciationRequest;
import com.acme.assets.v1.ApproveDepreciationResponse;
import com.acme.assets.v1.CommentFixedAssetApprovalRequest;
import com.acme.assets.v1.CommentFixedAssetApprovalResponse;
import com.acme.assets.v1.CompareDisposalOptionsRequest;
import com.acme.assets.v1.CompareDisposalOptionsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationApprovalServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.ExpireFixedAssetRequest;
import com.acme.assets.v1.ExpireFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetDimensions;
import com.acme.assets.v1.FixedAssetLifecycleServiceGrpc;
import com.acme.assets.v1.InspectFixedAssetWorkflowRequest;
import com.acme.assets.v1.InspectFixedAssetWorkflowResponse;
import com.acme.assets.v1.ListFixedAssetWorkflowsRequest;
import com.acme.assets.v1.ListFixedAssetWorkflowsResponse;
import com.acme.assets.v1.ResumeFixedAssetWorkflowRequest;
import com.acme.assets.v1.ResumeFixedAssetWorkflowResponse;
import com.acme.assets.v1.RetryFixedAssetWorkflowRequest;
import com.acme.assets.v1.RetryFixedAssetWorkflowResponse;
import com.acme.assets.v1.SignalFixedAssetWorkflowRequest;
import com.acme.assets.v1.SignalFixedAssetWorkflowResponse;
import com.acme.assets.v1.StartFixedAssetWorkflowRequest;
import com.acme.assets.v1.StartFixedAssetWorkflowResponse;
import com.acme.assets.v1.StreamDisposalMetricsRequest;
import com.acme.assets.v1.StreamDisposalMetricsResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featurestore.v1.FeatureView;
import com.acme.featurestore.v1.FeatureViewCalculationServiceGrpc;
import com.acme.featurestore.v1.FeatureViewDimensions;
import com.acme.featurestore.v1.QuoteFeatureViewRequest;
import com.acme.featurestore.v1.QuoteFeatureViewResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.RecallPolicyRequest;
import com.acme.insurance.v1.RecallPolicyResponse;
import com.acme.licensing.v1.FindSoftwareLicensesRequest;
import com.acme.licensing.v1.FindSoftwareLicensesResponse;
import com.acme.licensing.v1.SoftwareLicenseQueryServiceGrpc;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.WatchMetricSeriesesRequest;
import com.acme.metrics.v1.WatchMetricSeriesesResponse;
import com.acme.onboarding.v1.CountOnboardingPlansRequest;
import com.acme.onboarding.v1.CountOnboardingPlansResponse;
import com.acme.onboarding.v1.OnboardingPlanQueryServiceGrpc;
import com.acme.routing.v1.ExportRoutesRequest;
import com.acme.routing.v1.ExportRoutesResponse;
import com.acme.routing.v1.RouteQueryServiceGrpc;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a fixed asset from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class FixedAssetWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(FixedAssetWorkflowApplication.class);

    private final DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking;

    private final DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking;

    private final FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking;

    private final FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceBlockingStub assetsFixedAssetLifecycleServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final ChannelRegistry channels;

    private final FeatureViewCalculationServiceGrpc.FeatureViewCalculationServiceBlockingStub featurestoreFeatureViewCalculationServiceBlocking;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final SoftwareLicenseQueryServiceGrpc.SoftwareLicenseQueryServiceBlockingStub licensingSoftwareLicenseQueryServiceBlocking;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final OnboardingPlanQueryServiceGrpc.OnboardingPlanQueryServiceBlockingStub onboardingOnboardingPlanQueryServiceBlocking;

    private final AssetsRepository repository;

    private final RouteQueryServiceGrpc.RouteQueryServiceBlockingStub routingRouteQueryServiceBlocking;

    private final Executor rpcExecutor;

    public FixedAssetWorkflowApplication(
            DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking,
            DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking,
            FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking,
            FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceBlockingStub assetsFixedAssetLifecycleServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            ChannelRegistry channels,
            FeatureViewCalculationServiceGrpc.FeatureViewCalculationServiceBlockingStub featurestoreFeatureViewCalculationServiceBlocking,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            SoftwareLicenseQueryServiceGrpc.SoftwareLicenseQueryServiceBlockingStub licensingSoftwareLicenseQueryServiceBlocking,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            OnboardingPlanQueryServiceGrpc.OnboardingPlanQueryServiceBlockingStub onboardingOnboardingPlanQueryServiceBlocking,
            AssetsRepository repository,
            RouteQueryServiceGrpc.RouteQueryServiceBlockingStub routingRouteQueryServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsDisposalCalculationServiceBlocking = assetsDisposalCalculationServiceBlocking;
        this.assetsDisposalReportingServiceBlocking = assetsDisposalReportingServiceBlocking;
        this.assetsFixedAssetApprovalServiceBlocking = assetsFixedAssetApprovalServiceBlocking;
        this.assetsFixedAssetLifecycleServiceBlocking = assetsFixedAssetLifecycleServiceBlocking;
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.channels = channels;
        this.featurestoreFeatureViewCalculationServiceBlocking = featurestoreFeatureViewCalculationServiceBlocking;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.licensingSoftwareLicenseQueryServiceBlocking = licensingSoftwareLicenseQueryServiceBlocking;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.onboardingOnboardingPlanQueryServiceBlocking = onboardingOnboardingPlanQueryServiceBlocking;
        this.repository = repository;
        this.routingRouteQueryServiceBlocking = routingRouteQueryServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Starts a fixed asset.
     */
    public StartFixedAssetWorkflowResponse startFixedAssetWorkflow(StartFixedAssetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartFixedAssetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = ExportRoutesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportRoutesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFixedAsset().getAudit().getCreatedAt())
                        .setRegion(request.getFixedAsset().getAddress().getRegion())
                        .build())
                .build();
        var stream0 = routingRouteQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportRoutes(outbound0);
        var reply0 = ExportRoutesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = (ResolveRetentionPolicyConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveRetentionPolicyConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFixedAsset().getAudit().getCreatedAt())
                                .setCreatedBy(request.getFixedAsset().getAudit().getCreatedBy())
                                .build())
                        .setName(request.getFixedAsset().getName())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getFixedAsset().getOwner().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveRetentionPolicyConflict(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartFixedAssetWorkflow", request.getFixedAsset().getFixedAssetId());
        return StartFixedAssetWorkflowResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getRetentionPolicy().getAudit().getEtag())
                                .build())
                        .setFixedAssetId(request.getFixedAssetId())
                        .setOwner(request.getFixedAsset().getOwner())
                        .build())
                .build();
    }

    /**
     * Advances a fixed asset.
     */
    public AdvanceFixedAssetWorkflowResponse advanceFixedAssetWorkflow(AdvanceFixedAssetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceFixedAssetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = FindSoftwareLicensesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindSoftwareLicensesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFixedAsset().getAudit().getUpdatedAt())
                        .putAllLabels(request.getFixedAsset().getSpecsMap())
                        .setRegion(request.getFixedAsset().getAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> licensingSoftwareLicenseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findSoftwareLicenses(outbound0), rpcExecutor);

        var outbound1 = RecallPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallPolicyRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getFixedAsset().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallPolicy(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = WatchMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchMetricSeriesesRequest.Filter.newBuilder()
                        .setRegion(request.getFixedAsset().getAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getFixedAsset().getOwner().getEmail())
                        .setPhone(request.getDetails().getNotify().getPhone())
                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                        .build())
                .build();
        var stream2 = metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchMetricSerieses(outbound2);
        var reply2 = WatchMetricSeriesesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceFixedAssetWorkflow", request.getFixedAsset().getFixedAssetId());
        return AdvanceFixedAssetWorkflowResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .setStatus(request.getFixedAsset().getStatus())
                        .build())
                .setOutcome(AdvanceFixedAssetWorkflowResponse.Outcome.newBuilder()
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a fixed asset.
     */
    public RetryFixedAssetWorkflowResponse retryFixedAssetWorkflow(RetryFixedAssetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryFixedAssetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = JsonCodec.roundTrip(QuoteFeatureViewRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureView(FeatureView.newBuilder()
                        .setDimensions(FeatureViewDimensions.newBuilder()
                                .setWeight(request.getFixedAsset().getDimensions().getWeight())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getFixedAsset().getOwner().getEmail())
                                .build())
                        .build())
                .build(), QuoteFeatureViewRequest.class);
        var reply0 = featurestoreFeatureViewCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteFeatureView(outbound0);

        var outbound1 = CountOnboardingPlansRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountOnboardingPlansRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFixedAsset().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getFeatureView().getSpecsMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .setRegion(switch (request.getFixedAsset().getLocatorCase()) {
                            case ADDRESS -> request.getFixedAsset().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply1 = onboardingOnboardingPlanQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countOnboardingPlans(outbound1);

        var outbound2 = StreamDisposalMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamDisposalMetricsRequest.Filter.newBuilder()
                        .setRegion(request.getFixedAsset().getAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPhone(request.getDetails().getNotify().getPhone())
                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                        .build())
                .build();
        var stream2 = assetsDisposalReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalMetrics(outbound2);
        var reply2 = StreamDisposalMetricsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = ExpireFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExpireFixedAssetRequest.Details.newBuilder()
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getFeatureView().getOwner().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .setFixedAssetId(request.getFixedAssetId())
                .build();
        var adapter3 = new Function<ExpireFixedAssetRequest, ExpireFixedAssetResponse>() {
            @Override
            public ExpireFixedAssetResponse apply(ExpireFixedAssetRequest input) {
                return assetsFixedAssetLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).expireFixedAsset(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryFixedAssetWorkflow", request.getFixedAsset().getFixedAssetId());
        return RetryFixedAssetWorkflowResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getFeatureView().getAudit().getEtag())
                                .build())
                        .setFixedAssetId(request.getFixedAssetId())
                        .putAllSpecs(request.getFixedAsset().getSpecsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Resumes a fixed asset.
     */
    public ResumeFixedAssetWorkflowResponse resumeFixedAssetWorkflow(ResumeFixedAssetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeFixedAssetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = CommentFixedAssetApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(switch (request.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> request.getFixedAsset().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFixedAsset().getAudit().getCreatedAt())
                                .build())
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setDescription(request.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getDescription())
                                .build())
                        .setDimensions(FixedAssetDimensions.newBuilder()
                                .setWeight(request.getFixedAsset().getDimensions().getWeight())
                                .build())
                        .build())
                .build();
        var reply0 = assetsFixedAssetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentFixedAssetApproval(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeFixedAssetWorkflow", request.getFixedAsset().getFixedAssetId());
        return ResumeFixedAssetWorkflowResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getFixedAsset().getAudit().getCreatedAt())
                                .build())
                        .setCapacity(request.getFixedAsset().getCapacity())
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .build();
    }

    /**
     * Returns a fixed asset.
     */
    public InspectFixedAssetWorkflowResponse inspectFixedAssetWorkflow(InspectFixedAssetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectFixedAssetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "InspectFixedAssetWorkflow");

        var outbound0 = ResolveRetentionPolicyConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveRetentionPolicyConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .addAllScopes(request.getContext().getActor().getRolesList())
                        .build())
                .build();
        Function<ResolveRetentionPolicyConflictRequest, ResolveRetentionPolicyConflictResponse> operation0 = auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::resolveRetentionPolicyConflict;
        var reply0 = operation0.apply(outbound0);
        return InspectFixedAssetWorkflowResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRetentionPolicy().getAudit().getCreatedBy())
                                .build())
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .build();
    }

    /**
     * Lists fixed assets a page at a time.
     */
    public ListFixedAssetWorkflowsResponse listFixedAssetWorkflows(ListFixedAssetWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListFixedAssetWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getContext().getTraceId(), "ListFixedAssetWorkflows");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListFixedAssetWorkflows", request.getPage().getPageToken());
        return ListFixedAssetWorkflowsResponse.getDefaultInstance();
    }

    /**
     * Signals a fixed asset.
     */
    public SignalFixedAssetWorkflowResponse signalFixedAssetWorkflow(SignalFixedAssetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalFixedAssetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = CompareDisposalOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareDisposalOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getFixedAsset().getAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var reply0 = assetsDisposalCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareDisposalOptions(outbound0);

        var outbound1 = ApproveDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getDisposal().getRequester().getEmail())
                                        .setName(request.getDetails().getNotify().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.assets.v1.DepreciationApprovalService");
        var selected1 = DepreciationApprovalServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.approveDepreciation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalFixedAssetWorkflow", request.getFixedAsset().getFixedAssetId());
        return SignalFixedAssetWorkflowResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getDisposal().getAudit().getEtag())
                                .build())
                        .setFixedAssetId(request.getFixedAssetId())
                        .setName(request.getFixedAsset().getName())
                        .build())
                .setOutcome(SignalFixedAssetWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
