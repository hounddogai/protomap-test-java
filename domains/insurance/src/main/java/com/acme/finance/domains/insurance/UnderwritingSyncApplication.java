package com.acme.finance.domains.insurance;

import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.SearchFixedAssetsRequest;
import com.acme.assets.v1.SearchFixedAssetsResponse;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.Forum;
import com.acme.community.v1.ForumServiceGrpc;
import com.acme.community.v1.UpdateForumRequest;
import com.acme.community.v1.UpdateForumResponse;
import com.acme.content.v1.Article;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.ContentRevisionRef;
import com.acme.content.v1.UpdateArticleRequest;
import com.acme.content.v1.UpdateArticleResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimLifecycleServiceGrpc;
import com.acme.insurance.v1.ComparePremiumOptionsRequest;
import com.acme.insurance.v1.ComparePremiumOptionsResponse;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ExpirePolicyRequest;
import com.acme.insurance.v1.ExpirePolicyResponse;
import com.acme.insurance.v1.ExplainCoverageDecisionRequest;
import com.acme.insurance.v1.ExplainCoverageDecisionResponse;
import com.acme.insurance.v1.GetPolicyLifecycleRequest;
import com.acme.insurance.v1.GetPolicyLifecycleResponse;
import com.acme.insurance.v1.GetUnderwritingSyncStateRequest;
import com.acme.insurance.v1.GetUnderwritingSyncStateResponse;
import com.acme.insurance.v1.ListUnderwritingConflictsRequest;
import com.acme.insurance.v1.ListUnderwritingConflictsResponse;
import com.acme.insurance.v1.PolicyLifecycleServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumAttachment;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PreviewPremiumRequest;
import com.acme.insurance.v1.PreviewPremiumResponse;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.PullUnderwritingsRequest;
import com.acme.insurance.v1.PullUnderwritingsResponse;
import com.acme.insurance.v1.PushPremiumRequest;
import com.acme.insurance.v1.PushPremiumResponse;
import com.acme.insurance.v1.PushUnderwritingRequest;
import com.acme.insurance.v1.PushUnderwritingResponse;
import com.acme.insurance.v1.ReconcileUnderwritingsRequest;
import com.acme.insurance.v1.ReconcileUnderwritingsResponse;
import com.acme.insurance.v1.ResolveUnderwritingConflictRequest;
import com.acme.insurance.v1.ResolveUnderwritingConflictResponse;
import com.acme.insurance.v1.ScheduleClaimExpiryRequest;
import com.acme.insurance.v1.ScheduleClaimExpiryResponse;
import com.acme.insurance.v1.ScheduleCoverageReportRequest;
import com.acme.insurance.v1.ScheduleCoverageReportResponse;
import com.acme.insurance.v1.StreamPremiumChangesRequest;
import com.acme.insurance.v1.StreamPremiumChangesResponse;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.SyncUnderwritingsRequest;
import com.acme.insurance.v1.SyncUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SendPostingBatchTestNoticeRequest;
import com.acme.ledger.v1.SendPostingBatchTestNoticeResponse;
import com.acme.metrics.v1.LintMetricSeriesesRequest;
import com.acme.metrics.v1.LintMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureBankFunding;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.LintExposuresRequest;
import com.acme.risk.v1.LintExposuresResponse;
import com.acme.risk.v1.ValidateExposureRequest;
import com.acme.risk.v1.ValidateExposureResponse;
import com.acme.suppliers.v1.CountSupplierContractsRequest;
import com.acme.suppliers.v1.CountSupplierContractsResponse;
import com.acme.suppliers.v1.ResolveSupplierContractsRequest;
import com.acme.suppliers.v1.ResolveSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Keeps underwritings in step with the systems of record in other regions. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class UnderwritingSyncApplication {
    private static final Logger log = LoggerFactory.getLogger(UnderwritingSyncApplication.class);

    private final FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync;

    private final ForumServiceGrpc.ForumServiceBlockingStub communityForumServiceBlocking;

    private final ArticleServiceGrpc.ArticleServiceBlockingStub contentArticleServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ClaimLifecycleServiceGrpc.ClaimLifecycleServiceBlockingStub insuranceClaimLifecycleServiceBlocking;

    private final CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking;

    private final PolicyLifecycleServiceGrpc.PolicyLifecycleServiceBlockingStub insurancePolicyLifecycleServiceBlocking;

    private final PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceStub insurancePremiumSyncServiceAsync;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final InsuranceRepository repository;

    private final ExposureValidationServiceGrpc.ExposureValidationServiceBlockingStub riskExposureValidationServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    public UnderwritingSyncApplication(
            FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync,
            ForumServiceGrpc.ForumServiceBlockingStub communityForumServiceBlocking,
            ArticleServiceGrpc.ArticleServiceBlockingStub contentArticleServiceBlocking,
            ApplicationEventPublisher events,
            ClaimLifecycleServiceGrpc.ClaimLifecycleServiceBlockingStub insuranceClaimLifecycleServiceBlocking,
            CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking,
            PolicyLifecycleServiceGrpc.PolicyLifecycleServiceBlockingStub insurancePolicyLifecycleServiceBlocking,
            PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceStub insurancePremiumSyncServiceAsync,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            InsuranceRepository repository,
            ExposureValidationServiceGrpc.ExposureValidationServiceBlockingStub riskExposureValidationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking) {
        this.assetsFixedAssetServiceBlocking = assetsFixedAssetServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseServiceAsync = collectionsCollectionCaseServiceAsync;
        this.communityForumServiceBlocking = communityForumServiceBlocking;
        this.contentArticleServiceBlocking = contentArticleServiceBlocking;
        this.events = events;
        this.insuranceClaimLifecycleServiceBlocking = insuranceClaimLifecycleServiceBlocking;
        this.insuranceCoverageReportingServiceBlocking = insuranceCoverageReportingServiceBlocking;
        this.insurancePolicyLifecycleServiceBlocking = insurancePolicyLifecycleServiceBlocking;
        this.insurancePremiumCalculationServiceBlocking = insurancePremiumCalculationServiceBlocking;
        this.insurancePremiumSyncServiceAsync = insurancePremiumSyncServiceAsync;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.repository = repository;
        this.riskExposureValidationServiceBlocking = riskExposureValidationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
    }

    public record ReconcileUnderwritingsStage3Requested(PreviewPremiumRequest request, CompletableFuture<PreviewPremiumResponse> completion, io.grpc.Context context) {}

    /**
     * Syncs an underwriting.
     */
    public SyncUnderwritingsResponse syncUnderwritings(SyncUnderwritingsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SyncUnderwritings")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = CountSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSupplierContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getUnderwriting().getSla().getDueAt())
                        .putAllLabels(request.getUnderwriting().getCustomFieldsMap())
                        .setOwnerId(request.getUnderwriting().getSla().getPolicyId())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSupplierContracts(outbound0));

        var builder1 = LintMetricSeriesesRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setDetails(LintMetricSeriesesRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getUnderwriting().getAssignee().getEmail())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintMetricSerieses(outbound1);

        var results2 = new ArrayList<PullPremiumsResponse>();
        for (var element : request.getUnderwriting().getCommentsList()) {
            PullPremiumsRequest itemRequest = PullPremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullPremiumsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .build();
            results2.add(insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullPremiums(itemRequest));
        }
        var reply2 = results2.isEmpty() ? PullPremiumsResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = ExpirePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExpirePolicyRequest.Details.newBuilder()
                        .setEffectiveAt(reply2.getOutcome().getEffectiveAt())
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getUnderwriting().getAssignee().getDisplayName())
                                .setEmail(reply1.getMetricSeries().getOwner().getEmail())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(request.getUnderwriting().getAssignee().getEmail())
                                .build())
                        .build())
                .setPolicyId(request.getUnderwriting().getSla().getPolicyId())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> insurancePolicyLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).expirePolicy(value)).orElseThrow();

        var outbound4 = BatchCreateCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateCollectionCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getUnderwriting().getRequester())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateCollectionCasesResponse.getDefaultInstance());
        var input4 = collectionsCollectionCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCollectionCases(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamPremiumChangesRequest.newBuilder()
                .setChange(Premium.newBuilder()
                        .addAttachments(PremiumAttachment.newBuilder()
                                .setChecksum(request.getDetails().getChecksum())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getUnderwriting().getAudit().getCreatedAt())
                                .setCreatedBy(request.getUnderwriting().getAudit().getCreatedBy())
                                .setEtag(request.getUnderwriting().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamPremiumChangesResponse.getDefaultInstance());
        var input5 = insurancePremiumSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamPremiumChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SyncUnderwritings", request.getUnderwriting().getUnderwritingId());
        return SyncUnderwritingsResponse.newBuilder()
                .setOutcome(SyncUnderwritingsResponse.Outcome.newBuilder()
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply1.getMetricSeries().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getMetricSeries().getAudit().getUpdatedBy())
                                .build())
                        .putAllCustomFields(request.getUnderwriting().getCustomFieldsMap())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Pushs an underwriting.
     */
    public PushUnderwritingResponse pushUnderwriting(PushUnderwritingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PushUnderwriting")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = ExplainCoverageDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainCoverageDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getUnderwriting().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.CoverageValidationService");
        var selected0 = CoverageValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.explainCoverageDecision(outbound0);

        var outbound1 = (SearchFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchFixedAssetsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getCoverage().getAudit().getUpdatedAt())
                        .putAllLabels(request.getUnderwriting().getCustomFieldsMap())
                        .setOwnerId(request.getUnderwriting().getSla().getPolicyId())
                        .build())
                .build()).toBuilder().build();
        var reply1 = assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchFixedAssets(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PushUnderwriting", request.getUnderwriting().getUnderwritingId());
        return PushUnderwritingResponse.newBuilder()
                .setOutcome(PushUnderwritingResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setSubject(request.getUnderwriting().getSubject())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Pulls an underwriting.
     */
    public PullUnderwritingsResponse pullUnderwritings(PullUnderwritingsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PullUnderwritings")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = ResolveSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveSupplierContracts(outbound0), rpcExecutor);

        var outbound1 = PushPremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushPremiumRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getUnderwriting().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getUnderwriting().getAssignee().getEmail())
                                .build())
                        .setDueAt(request.getUnderwriting().getSla().getDueAt())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushPremium(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = ValidateExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateExposureRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getUnderwriting().getRequester().getEmail())
                                .build())
                        .build())
                .setExposure(Exposure.newBuilder()
                        .setBank(ExposureBankFunding.newBuilder()
                                .setIban(reply1.getPremium().getBank().getIban())
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(request.getUnderwriting().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = riskExposureValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateExposure(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PullUnderwritings", request.getUnderwriting().getUnderwritingId());
        return PullUnderwritingsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPremium().getAudit().getEtag())
                                .build())
                        .putAllCustomFields(request.getUnderwriting().getCustomFieldsMap())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply2.getExposure().getContact().getEmail())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Reconciles an underwriting.
     */
    public ReconcileUnderwritingsResponse reconcileUnderwritings(ReconcileUnderwritingsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReconcileUnderwritings")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = GetPolicyLifecycleRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicyId(request.getUnderwriting().getSla().getPolicyId())
                .build();
        RpcPort<GetPolicyLifecycleRequest, GetPolicyLifecycleResponse> port0 = RpcPort.adapt(insurancePolicyLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::getPolicyLifecycle);
        var reply0 = port0.exchange(outbound0);

        var results1 = new ArrayList<ScheduleClaimExpiryResponse>();
        for (var element : request.getUnderwriting().getCommentsList()) {
            ScheduleClaimExpiryRequest itemRequest = ScheduleClaimExpiryRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .addComments(ClaimComment.newBuilder()
                                .setAuthorEmail(element.getAuthorEmail())
                                .setVisibility(element.getVisibility())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results1.add(insuranceClaimLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleClaimExpiry(itemRequest));
        }
        var reply1 = results1.isEmpty() ? ScheduleClaimExpiryResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = ScheduleCoverageReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setCoverageId(reply0.getPolicy().getHolder().getCoverageId())
                .setReason(request.getReason())
                .build();
        RpcPort<ScheduleCoverageReportRequest, ScheduleCoverageReportResponse> port2 = RpcPort.adapt(insuranceCoverageReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scheduleCoverageReport);
        var reply2 = port2.exchange(outbound2);

        var outbound3 = PreviewPremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getClaim().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var completion3 = new CompletableFuture<PreviewPremiumResponse>();
        events.publishEvent(new ReconcileUnderwritingsStage3Requested(outbound3, completion3, io.grpc.Context.current()));
        var reply3 = completion3.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReconcileUnderwritings", request.getUnderwriting().getUnderwritingId());
        return ReconcileUnderwritingsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply3.getOutcome().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPolicy().getAudit().getEtag())
                                .build())
                        .setStatus(request.getUnderwriting().getStatus())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Exchanges changes of underwritings with a peer.
     */
    public StreamUnderwritingChangesResponse streamUnderwritingChanges(StreamUnderwritingChangesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamUnderwritingChanges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasChange()) {
            repository.saveUnderwriting(request.getContext().getTenantId(), request.getChange());
        }

        var results0 = new ArrayList<UpdateForumResponse>();
        for (var element : request.getChange().getCommentsList()) {
            UpdateForumRequest itemRequest = UpdateForumRequest.newBuilder()
                .setContext(request.getContext())
                .setForum(Forum.newBuilder()
                        .addChildren(Forum.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setCreatedAt(element.getCreatedAt())
                                        .build())
                                .build())
                        .build())
                .build();
            results0.add(communityForumServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateForum(itemRequest));
        }
        var reply0 = results0.isEmpty() ? UpdateForumResponse.getDefaultInstance()
                : results0.getLast();

        var results1 = new ArrayList<SendPostingBatchTestNoticeResponse>();
        for (var element : request.getChange().getCommentsList()) {
            SendPostingBatchTestNoticeRequest itemRequest = SendPostingBatchTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendPostingBatchTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build();
            results1.add(ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendPostingBatchTestNotice(itemRequest));
        }
        var reply1 = results1.isEmpty() ? SendPostingBatchTestNoticeResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getChange().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamUnderwritingChanges", request.getChange().getUnderwritingId());
        return StreamUnderwritingChangesResponse.newBuilder()
                .setSequence(request.getAck().getSequence())
                .setUnderwriting(Underwriting.newBuilder()
                        .setSubject(request.getChange().getSubject())
                        .build())
                .build();
    }

    /**
     * Resolves an underwriting.
     */
    public ResolveUnderwritingConflictResponse resolveUnderwritingConflict(ResolveUnderwritingConflictRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveUnderwritingConflict")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = ComparePremiumOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getUnderwriting().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(request.getUnderwriting().getCustomer().getDisplayName())
                                .setEmail(request.getUnderwriting().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = insurancePremiumCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePremiumOptions(outbound0);

        var outbound1 = UpdateArticleRequest.newBuilder()
                .setArticle(Article.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPremium().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(ContentRevisionRef.newBuilder()
                                .setDisplayName(switch (request.getUnderwriting().getOriginCase()) {
                                    case CUSTOMER -> request.getUnderwriting().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = contentArticleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateArticle(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResolveUnderwritingConflict", request.getUnderwriting().getUnderwritingId());
        return ResolveUnderwritingConflictResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .putAllCustomFields(request.getUnderwriting().getCustomFieldsMap())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Returns an underwriting.
     */
    public GetUnderwritingSyncStateResponse getUnderwritingSyncState(GetUnderwritingSyncStateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetUnderwritingSyncState")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getUnderwritingId(), "GetUnderwritingSyncState");

        var outbound0 = LintExposuresRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintExposuresRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        var reply0 = riskExposureValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintExposures(outbound0);
        return GetUnderwritingSyncStateResponse.newBuilder()
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getExposure().getAudit().getUpdatedAt())
                                .build())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Lists underwritings a page at a time.
     */
    public ListUnderwritingConflictsResponse listUnderwritingConflicts(ListUnderwritingConflictsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListUnderwritingConflicts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getContext().getTraceId(), "ListUnderwritingConflicts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListUnderwritingConflicts", request.getPage().getPageToken());
        return ListUnderwritingConflictsResponse.getDefaultInstance();
    }
}
