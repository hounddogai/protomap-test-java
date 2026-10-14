package com.acme.finance.domains.assets;

import com.acme.assets.v1.CalculateDepreciationRequest;
import com.acme.assets.v1.CalculateDepreciationResponse;
import com.acme.assets.v1.CalculateFixedAssetRequest;
import com.acme.assets.v1.CalculateFixedAssetResponse;
import com.acme.assets.v1.CompareDepreciationOptionsRequest;
import com.acme.assets.v1.CompareDepreciationOptionsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.EstimateDepreciationRequest;
import com.acme.assets.v1.EstimateDepreciationResponse;
import com.acme.assets.v1.ExplainDepreciationCalculationRequest;
import com.acme.assets.v1.ExplainDepreciationCalculationResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.GetDepreciationRatesRequest;
import com.acme.assets.v1.GetDepreciationRatesResponse;
import com.acme.assets.v1.QuoteDepreciationRequest;
import com.acme.assets.v1.QuoteDepreciationResponse;
import com.acme.assets.v1.RecalculateDepreciationsRequest;
import com.acme.assets.v1.RecalculateDepreciationsResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.assets.v1.SimulateDepreciationRequest;
import com.acme.assets.v1.SimulateDepreciationResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityRequest;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityResponse;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.ExportForumsRequest;
import com.acme.community.v1.ExportForumsResponse;
import com.acme.community.v1.ForumQueryServiceGrpc;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.ValidateAttestationRequest;
import com.acme.compliance.v1.ValidateAttestationResponse;
import com.acme.compliance.v1.ValidateSanctionRequest;
import com.acme.compliance.v1.ValidateSanctionResponse;
import com.acme.deliveries.v1.CloneDeliveryRequest;
import com.acme.deliveries.v1.CloneDeliveryResponse;
import com.acme.deliveries.v1.Delivery;
import com.acme.deliveries.v1.DeliveryExceptionRef;
import com.acme.deliveries.v1.DeliveryLine;
import com.acme.deliveries.v1.DeliveryServiceGrpc;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagBlockAction;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.NormalizeFeatureFlagRequest;
import com.acme.featureflags.v1.NormalizeFeatureFlagResponse;
import com.acme.featureflags.v1.ValidateFeatureFlagRequest;
import com.acme.featureflags.v1.ValidateFeatureFlagResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.BatchGetUsersRequest;
import com.acme.identity.v1.BatchGetUsersResponse;
import com.acme.identity.v1.UpdateUserRequest;
import com.acme.identity.v1.UpdateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.PreviewCoverageRequest;
import com.acme.insurance.v1.PreviewCoverageResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.EstimateCollateralRequest;
import com.acme.lending.v1.EstimateCollateralResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.RecalculateLoanApplicationsRequest;
import com.acme.lending.v1.RecalculateLoanApplicationsResponse;
import com.acme.metrics.v1.LintMetricSeriesesRequest;
import com.acme.metrics.v1.LintMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.yards.v1.CountYardsRequest;
import com.acme.yards.v1.CountYardsResponse;
import com.acme.yards.v1.YardQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
 * Computes the figures of a depreciation, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class DepreciationCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(DepreciationCalculationApplication.class);

    private final FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking;

    private final ForumQueryServiceGrpc.ForumQueryServiceBlockingStub communityForumQueryServiceBlocking;

    private final SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking;

    private final DeliveryServiceGrpc.DeliveryServiceBlockingStub deliveriesDeliveryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking;

    private final LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking;

    private final DepreciationCalculationRequestMapper mapper;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final AssetsRepository repository;

    private final Executor rpcExecutor;

    private final YardQueryServiceGrpc.YardQueryServiceBlockingStub yardsYardQueryServiceBlocking;

    public DepreciationCalculationApplication(
            FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking,
            ForumQueryServiceGrpc.ForumQueryServiceBlockingStub communityForumQueryServiceBlocking,
            SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking,
            DeliveryServiceGrpc.DeliveryServiceBlockingStub deliveriesDeliveryServiceBlocking,
            ApplicationEventPublisher events,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking,
            LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking,
            DepreciationCalculationRequestMapper mapper,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            AssetsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            YardQueryServiceGrpc.YardQueryServiceBlockingStub yardsYardQueryServiceBlocking) {
        this.assetsFixedAssetCalculationServiceBlocking = assetsFixedAssetCalculationServiceBlocking;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.auditRetentionPolicyValidationServiceBlocking = auditRetentionPolicyValidationServiceBlocking;
        this.communityForumQueryServiceBlocking = communityForumQueryServiceBlocking;
        this.complianceSanctionValidationServiceBlocking = complianceSanctionValidationServiceBlocking;
        this.deliveriesDeliveryServiceBlocking = deliveriesDeliveryServiceBlocking;
        this.events = events;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.lendingCollateralCalculationServiceBlocking = lendingCollateralCalculationServiceBlocking;
        this.lendingLoanApplicationCalculationServiceBlocking = lendingLoanApplicationCalculationServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.yardsYardQueryServiceBlocking = yardsYardQueryServiceBlocking;
    }

    public record CalculateDepreciationStage0Requested(ValidateAttestationRequest request, CompletableFuture<ValidateAttestationResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates a depreciation.
     */
    public CalculateDepreciationResponse calculateDepreciation(CalculateDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = ValidateAttestationRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getDepreciation().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .setRegion(request.getDepreciation().getLocation().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<ValidateAttestationResponse>();
        events.publishEvent(new CalculateDepreciationStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = ValidateSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getDepreciation().getAudit().getCreatedAt())
                                                .setCreatedBy(reply0.getAttestation().getAudit().getCreatedBy())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = complianceSanctionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSanction(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateDepreciation", request.getDepreciation().getDepreciationId());
        return CalculateDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getAttestation().getAudit().getUpdatedAt())
                                .build())
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getSanction().getLocation().getCountryCode())
                                .addAllLines(reply1.getSanction().getLocation().getLinesList())
                                .build())
                        .setWindow(request.getDepreciation().getWindow())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Estimates a depreciation.
     */
    public EstimateDepreciationResponse estimateDepreciation(EstimateDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var results0 = new ArrayList<ValidateFeatureFlagResponse>();
        for (var element : request.getDepreciation().getParticipantsList()) {
            ValidateFeatureFlagRequest itemRequest = ValidateFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateFeatureFlagRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .setName(element.getContact().getName())
                                .setPhone(element.getContact().getPhone())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
            results0.add(featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateFeatureFlag(itemRequest));
        }
        var reply0 = results0.isEmpty() ? ValidateFeatureFlagResponse.getDefaultInstance()
                : results0.getLast();

        var futures1 = request.getDepreciation().getParticipantsList().stream()
                .map(element -> EstimateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCollateral(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? EstimateCollateralResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = NormalizeFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setBlock(FeatureFlagBlockAction.newBuilder()
                                .setReason(switch (reply0.getFeatureFlag().getActionCase()) {
                                    case BLOCK -> reply0.getFeatureFlag().getBlock().getReason();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeFeatureFlag(outbound2);

        var outbound3 = mapper.estimateDepreciationStage3(request, reply0, reply1);
        var reply3 = deliveriesDeliveryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneDelivery(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateDepreciation", request.getDepreciation().getDepreciationId());
        return EstimateDepreciationResponse.newBuilder()
                .setBreakdown(EstimateDepreciationResponse.Breakdown.newBuilder()
                        .setTotal(reply3.getDelivery().getTotal())
                        .build())
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(request.getDepreciation().getLocation())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(reply2.getFeatureFlag().getOwner().getUserId())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Quotes a depreciation.
     */
    public QuoteDepreciationResponse quoteDepreciation(QuoteDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = BeanCopies.copy(RecalculateLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getDepreciation().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), RecalculateLoanApplicationsRequest.class);
        var reply0 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateLoanApplications(outbound0);

        var outbound1 = BeanCopies.copy(PreviewCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDepreciation().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getLoanApplication().getAudit().getCreatedBy())
                                .setEtag(request.getDepreciation().getAudit().getEtag())
                                .build())
                        .build())
                .setDetails(PreviewCoverageRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDepreciation().getOrganizer().getEmail())
                                .build())
                        .build())
                .build(), PreviewCoverageRequest.class);
        var reply1 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewCoverage(outbound1);

        List<LintMetricSeriesesResponse> results2 = request.getDepreciation().getParticipantsList().stream()
                .map(element -> LintMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintMetricSeriesesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(element.getContact().getName())
                                .build())
                        .build())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintMetricSerieses(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? LintMetricSeriesesResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteDepreciation", request.getDepreciation().getDepreciationId());
        return QuoteDepreciationResponse.newBuilder()
                .setBreakdown(QuoteDepreciationResponse.Breakdown.newBuilder()
                        .addLines(QuoteDepreciationResponse.Breakdown.Line.newBuilder()
                                .setAmount(reply1.getCoverage().getDiscount().getAmount())
                                .build())
                        .build())
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(reply0.getLoanApplication().getBillingAddress().getRegion())
                                .build())
                        .setWindow(request.getDepreciation().getWindow())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Compares a depreciation.
     */
    public CompareDepreciationOptionsResponse compareDepreciationOptions(CompareDepreciationOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareDepreciationOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var futures0 = request.getDepreciation().getParticipantsList().stream()
                .map(element -> RecalculateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateFixedAssets(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? RecalculateFixedAssetsResponse.getDefaultInstance()
                : results0.getLast();

        var builder1 = BatchGetUsersRequest.newBuilder();
        builder1.setContext(request.getContext());
        var outbound1 = builder1.build();
        var reply1 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetUsers(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareDepreciationOptions", request.getDepreciation().getDepreciationId());
        return CompareDepreciationOptionsResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setTitle(request.getDepreciation().getTitle())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Recalculates a depreciation.
     */
    public RecalculateDepreciationsResponse recalculateDepreciations(RecalculateDepreciationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateDepreciations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = CountYardsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountYardsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDepreciation().getAudit().getUpdatedAt())
                        .setOwnerId(request.getDepreciation().getOrganizer().getUserId())
                        .setRegion(request.getDepreciation().getLocation().getRegion())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> yardsYardQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countYards(value)).orElseThrow();

        var futures1 = request.getDepreciation().getParticipantsList().stream()
                .map(element -> CheckRetentionPolicyEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckRetentionPolicyEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .setName(element.getContact().getName())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRetentionPolicyEligibility(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? CheckRetentionPolicyEligibilityResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateDepreciations", request.getDepreciation().getDepreciationId());
        return RecalculateDepreciationsResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setOrganizer(request.getDepreciation().getOrganizer())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Simulates a depreciation.
     */
    public SimulateDepreciationResponse simulateDepreciation(SimulateDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = ExportForumsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportForumsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDepreciation().getAudit().getCreatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var stream0 = communityForumQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportForums(outbound0);
        var reply0 = ExportForumsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = ExplainFixedAssetCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getDepreciation().getLocation().getRecipient())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            ExplainFixedAssetCalculationResponse fetch(ExplainFixedAssetCalculationRequest input) {
                return assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainFixedAssetCalculation(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = CalculateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateFixedAssetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply1.getFixedAsset().getAddress().getLinesList())
                                .setRecipient(request.getDepreciation().getLocation().getRecipient())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRecipient(switch (reply1.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> reply1.getFixedAsset().getAddress().getRecipient();
                                    default -> "";
                                })
                                .setRegion(switch (reply1.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> reply1.getFixedAsset().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateFixedAsset(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateDepreciation", request.getDepreciation().getDepreciationId());
        return SimulateDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getFixedAsset().getAudit().getCreatedBy())
                                .build())
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getFixedAsset().getAddress().getCountryCode())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply1.getFixedAsset().getOwner().getEmail())
                                .build())
                        .setStatus(request.getDepreciation().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a depreciation.
     */
    public ExplainDepreciationCalculationResponse explainDepreciationCalculation(ExplainDepreciationCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainDepreciationCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = RpcSupport.unpack(Any.pack(NormalizeAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getDepreciation().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeAuditEventRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .setStrict(request.getDetails().getStrict())
                        .build())
                .build()), NormalizeAuditEventRequest.class);
        var reply0 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAuditEvent(outbound0);

        var outbound1 = RecalculateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateFixedAssetsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDepreciation().getLocation().getLocality())
                                .setRegion(request.getDepreciation().getLocation().getRegion())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDepreciation().getLocation().getCountryCode())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDepreciation().getOrganizer().getEmail())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<RecalculateFixedAssetsRequest, RecalculateFixedAssetsResponse>() {
            @Override
            public RecalculateFixedAssetsResponse apply(RecalculateFixedAssetsRequest input) {
                return assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateFixedAssets(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainDepreciationCalculation", request.getDepreciation().getDepreciationId());
        return ExplainDepreciationCalculationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(reply1.getFixedAsset().getAddress().getLocality())
                                .build())
                        .setOrganizer(request.getDepreciation().getOrganizer())
                        .build())
                .setOutcome(ExplainDepreciationCalculationResponse.Outcome.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a depreciation.
     */
    public GetDepreciationRatesResponse getDepreciationRates(GetDepreciationRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDepreciationRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "GetDepreciationRates");

        var outbound0 = UpdateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateUser(value)).orElseThrow();
        return GetDepreciationRatesResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
