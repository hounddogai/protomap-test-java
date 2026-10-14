package com.acme.finance.domains.assets;

import com.acme.assets.v1.CalculateFixedAssetRequest;
import com.acme.assets.v1.CalculateFixedAssetResponse;
import com.acme.assets.v1.CheckDepreciationEligibilityRequest;
import com.acme.assets.v1.CheckDepreciationEligibilityResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.ExplainDepreciationDecisionRequest;
import com.acme.assets.v1.ExplainDepreciationDecisionResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GetDepreciationRulesRequest;
import com.acme.assets.v1.GetDepreciationRulesResponse;
import com.acme.assets.v1.LintDepreciationsRequest;
import com.acme.assets.v1.LintDepreciationsResponse;
import com.acme.assets.v1.LintFixedAssetsRequest;
import com.acme.assets.v1.LintFixedAssetsResponse;
import com.acme.assets.v1.NormalizeDepreciationRequest;
import com.acme.assets.v1.NormalizeDepreciationResponse;
import com.acme.assets.v1.PreviewDepreciationRequest;
import com.acme.assets.v1.PreviewDepreciationResponse;
import com.acme.assets.v1.QuoteDepreciationRequest;
import com.acme.assets.v1.QuoteDepreciationResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.assets.v1.ScoreDepreciationRequest;
import com.acme.assets.v1.ScoreDepreciationResponse;
import com.acme.assets.v1.ScoreFixedAssetRequest;
import com.acme.assets.v1.ScoreFixedAssetResponse;
import com.acme.assets.v1.ValidateDepreciationRequest;
import com.acme.assets.v1.ValidateDepreciationResponse;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.NormalizeAccessPolicyRequest;
import com.acme.authorization.v1.NormalizeAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.ValidateSanctionRequest;
import com.acme.compliance.v1.ValidateSanctionResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PreviewUnderwritingRequest;
import com.acme.insurance.v1.PreviewUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.lending.v1.ExportLoanApplicationsRequest;
import com.acme.lending.v1.ExportLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.risk.v1.ExplainExposureCalculationRequest;
import com.acme.risk.v1.ExplainExposureCalculationResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.WatchRiskRegistersRequest;
import com.acme.risk.v1.WatchRiskRegistersResponse;
import com.acme.tours.v1.CountToursRequest;
import com.acme.tours.v1.CountToursResponse;
import com.acme.tours.v1.TourQueryServiceGrpc;
import com.acme.venues.v1.BrowseVenuesRequest;
import com.acme.venues.v1.BrowseVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.acme.workplaces.v1.BuildingServiceGrpc;
import com.acme.workplaces.v1.WatchBuildingsRequest;
import com.acme.workplaces.v1.WatchBuildingsResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
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
 * Checks depreciations against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class DepreciationValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(DepreciationValidationApplication.class);

    private final DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking;

    private final FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking;

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking;

    private final UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking;

    private final LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking;

    private final AssetsRepository repository;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceBlockingStub riskRiskRegisterServiceBlocking;

    private final Executor rpcExecutor;

    private final TourQueryServiceGrpc.TourQueryServiceBlockingStub toursTourQueryServiceBlocking;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    private final BuildingServiceGrpc.BuildingServiceBlockingStub workplacesBuildingServiceBlocking;

    public DepreciationValidationApplication(
            DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking,
            FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking,
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking,
            UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking,
            LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking,
            AssetsRepository repository,
            RiskRegisterServiceGrpc.RiskRegisterServiceBlockingStub riskRiskRegisterServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            TourQueryServiceGrpc.TourQueryServiceBlockingStub toursTourQueryServiceBlocking,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking,
            BuildingServiceGrpc.BuildingServiceBlockingStub workplacesBuildingServiceBlocking) {
        this.assetsDepreciationCalculationServiceBlocking = assetsDepreciationCalculationServiceBlocking;
        this.assetsFixedAssetCalculationServiceBlocking = assetsFixedAssetCalculationServiceBlocking;
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.complianceSanctionValidationServiceBlocking = complianceSanctionValidationServiceBlocking;
        this.insuranceUnderwritingValidationServiceBlocking = insuranceUnderwritingValidationServiceBlocking;
        this.lendingLoanApplicationQueryServiceBlocking = lendingLoanApplicationQueryServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterServiceBlocking = riskRiskRegisterServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.toursTourQueryServiceBlocking = toursTourQueryServiceBlocking;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
        this.workplacesBuildingServiceBlocking = workplacesBuildingServiceBlocking;
    }

    /**
     * Validates a depreciation.
     */
    public ValidateDepreciationResponse validateDepreciation(ValidateDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        List<QuoteComplianceCaseResponse> results0 = request.getDepreciation().getParticipantsList().stream()
                .map(element -> QuoteComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteComplianceCase(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? QuoteComplianceCaseResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RecalculateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getComplianceCase().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getComplianceCase().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateFixedAssets(value));

        var futures2 = request.getDepreciation().getParticipantsList().stream()
                .map(element -> ScoreFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreFixedAssetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDepreciation().getLocation().getLocality())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setDescription(reply1.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getDescription())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .setUserId(reply1.getFixedAsset().getOwner().getUserId())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreFixedAsset(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? ScoreFixedAssetResponse.getDefaultInstance()
                : results2.getLast();

        var futures3 = request.getDepreciation().getParticipantsList().stream()
                .map(element -> EstimateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getDepreciation().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFixedAsset(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? EstimateFixedAssetResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateDepreciation", request.getDepreciation().getDepreciationId());
        return ValidateDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getFixedAsset().getAudit().getUpdatedAt())
                                .build())
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getFixedAsset().getAddress().getCountryCode())
                                .build())
                        .putAllMetadata(request.getDepreciation().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Previews a depreciation.
     */
    public PreviewDepreciationResponse previewDepreciation(PreviewDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = ExplainFixedAssetCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainFixedAssetCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getDepreciation().getLocation().getPostalCode())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDepreciation().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainFixedAssetCalculationRequest, ExplainFixedAssetCalculationResponse> port0 = RpcPort.adapt(assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainFixedAssetCalculation);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = SuggestAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getOutcome().getEffectiveAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(reply0.getFixedAsset().getOwner().getUserId())
                        .setRegion(request.getDepreciation().getLocation().getRegion())
                        .build())
                .build();
        final class Stage1Adapter {
            SuggestAuditEventsResponse fetch(SuggestAuditEventsRequest input) {
                return auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestAuditEvents(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewDepreciation", request.getDepreciation().getDepreciationId());
        return PreviewDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply0.getFixedAsset().getAddress().getLinesList())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(reply0.getFixedAsset().getOwner().getUserId())
                                .build())
                        .setTitle(request.getDepreciation().getTitle())
                        .build())
                .setOutcome(PreviewDepreciationResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Checks a depreciation.
     */
    public CheckDepreciationEligibilityResponse checkDepreciationEligibility(CheckDepreciationEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckDepreciationEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var results0 = new ArrayList<WatchBuildingsResponse>();
        for (var element : request.getDepreciation().getParticipantsList()) {
            WatchBuildingsRequest itemRequest = WatchBuildingsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPhone(element.getContact().getPhone())
                        .build())
                .build();
            var responses = workplacesBuildingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchBuildings(itemRequest);
            responses.forEachRemaining(results0::add);
        }
        var reply0 = results0.isEmpty() ? WatchBuildingsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CountToursRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountToursRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDepreciation().getAudit().getUpdatedAt())
                        .putAllLabels(request.getDepreciation().getMetadataMap())
                        .setOwnerId(request.getDepreciation().getDepreciationId())
                        .setRegion(request.getDepreciation().getLocation().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> toursTourQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countTours(value));
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getTitle()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckDepreciationEligibility", request.getDepreciation().getTitle());
        return CheckDepreciationEligibilityResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setStatus(request.getDepreciation().getStatus())
                        .build())
                .setOutcome(CheckDepreciationEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Normalizes a depreciation.
     */
    public NormalizeDepreciationResponse normalizeDepreciation(NormalizeDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        List<EstimateFixedAssetResponse> results0 = request.getDepreciation().getParticipantsList().stream()
                .map(element -> EstimateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateFixedAssetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDepreciation().getLocation().getCountryCode())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getDepreciation().getLocation().getRegion())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFixedAsset(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? EstimateFixedAssetResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = BrowseVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseVenuesRequest.Filter.newBuilder()
                        .setRegion(request.getDepreciation().getLocation().getRegion())
                        .build())
                .build();
        RpcPort<BrowseVenuesRequest, BrowseVenuesResponse> port1 = RpcPort.adapt(venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::browseVenues);
        var reply1 = port1.exchange(outbound1);

        var results2 = new ArrayList<LintFixedAssetsResponse>();
        for (var element : request.getDepreciation().getParticipantsList()) {
            LintFixedAssetsRequest itemRequest = LintFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintFixedAssetsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(element.getContact().getPhone())
                                .build())
                        .build())
                .build();
            results2.add(assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintFixedAssets(itemRequest));
        }
        var reply2 = results2.isEmpty() ? LintFixedAssetsResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = JsonCodec.roundTrip(NormalizeAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getFixedAsset().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getFixedAsset().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), NormalizeAccessPolicyRequest.class);
        var reply3 = authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAccessPolicy(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeDepreciation", request.getDepreciation().getDepreciationId());
        return NormalizeDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(request.getDepreciation().getLocation())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply3.getAccessPolicy().getOwner().getEmail())
                                .build())
                        .build())
                .setOutcome(NormalizeDepreciationResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Explains a depreciation.
     */
    public ExplainDepreciationDecisionResponse explainDepreciationDecision(ExplainDepreciationDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainDepreciationDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = ExplainExposureCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainExposureCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.risk.v1.ExposureCalculationService");
        var serviceName0 = "acme.risk.v1" + "." + "ExposureCalculationService";
        var methodName0 = "Exp" + "lainExposureCalculation";
        var descriptor0 = MethodDescriptor.<ExplainExposureCalculationRequest, ExplainExposureCalculationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ExplainExposureCalculationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExplainExposureCalculationResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = LintFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintFixedAssetsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getExposure().getHolder().getEmail())
                                .setPhone(reply0.getExposure().getContact().getPhone())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        RpcPort<LintFixedAssetsRequest, LintFixedAssetsResponse> port1 = RpcPort.adapt(assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintFixedAssets);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = ExportLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportLoanApplicationsRequest.Filter.newBuilder()
                        .putAllLabels(reply0.getExposure().getLabelsMap())
                        .setRegion(reply1.getFixedAsset().getAddress().getRegion())
                        .build())
                .build();
        var stream2 = lendingLoanApplicationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportLoanApplications(outbound2);
        var reply2 = ExportLoanApplicationsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var futures3 = request.getDepreciation().getParticipantsList().stream()
                .map(element -> CalculateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDepreciation().getAudit().getCreatedAt())
                                .setCreatedBy(request.getDepreciation().getAudit().getCreatedBy())
                                .build())
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setDescription(reply1.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getDescription())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateFixedAsset(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? CalculateFixedAssetResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainDepreciationDecision", request.getDepreciation().getDepreciationId());
        return ExplainDepreciationDecisionResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getFixedAsset().getAddress().getPostalCode())
                                .build())
                        .putAllMetadata(request.getDepreciation().getMetadataMap())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getExposure().getHolder().getEmail())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Scores a depreciation.
     */
    public ScoreDepreciationResponse scoreDepreciation(ScoreDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var results0 = new ArrayList<WatchRiskRegistersResponse>();
        for (var element : request.getDepreciation().getParticipantsList()) {
            WatchRiskRegistersRequest itemRequest = WatchRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(element.getContact().getEmail())
                        .setPhone(element.getContact().getPhone())
                        .build())
                .build();
            var responses = riskRiskRegisterServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchRiskRegisters(itemRequest);
            responses.forEachRemaining(results0::add);
        }
        var reply0 = results0.isEmpty() ? WatchRiskRegistersResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ExplainFixedAssetCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getDepreciation().getLocation().getRegion())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDepreciation().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainFixedAssetCalculation(value)))
                .collect(Collectors.toList()).getFirst();

        List<LintFixedAssetsResponse> results2 = request.getDepreciation().getParticipantsList().stream()
                .map(element -> LintFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintFixedAssetsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintFixedAssets(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? LintFixedAssetsResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreDepreciation", request.getDepreciation().getDepreciationId());
        return ScoreDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(reply1.getFixedAsset().getAddress().getLocality())
                                .build())
                        .putAllMetadata(request.getDepreciation().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Lints a depreciation.
     */
    public LintDepreciationsResponse lintDepreciations(LintDepreciationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintDepreciations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDepreciation(request.getContext().getTenantId(), request.getDepreciation());

        var outbound0 = ValidateSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setEtag(request.getDepreciation().getAudit().getEtag())
                                        .build())
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(request.getDepreciation().getAudit().getUpdatedAt())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getDepreciation().getLocation().getRegion())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getDepreciation().getOrganizer().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        Function<ValidateSanctionRequest, ValidateSanctionResponse> operation0 = complianceSanctionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateSanction;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDepreciation().getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintDepreciations", request.getDepreciation().getDepreciationId());
        return LintDepreciationsResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply0.getSanction().getLocation().getLinesList())
                                .build())
                        .setWindow(request.getDepreciation().getWindow())
                        .build())
                .build();
    }

    /**
     * Returns a depreciation.
     */
    public GetDepreciationRulesResponse getDepreciationRules(GetDepreciationRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDepreciationRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "GetDepreciationRules");

        var outbound0 = JsonCodec.roundTrip(PreviewUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewUnderwritingRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build(), PreviewUnderwritingRequest.class);
        var reply0 = insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewUnderwriting(outbound0);

        var outbound1 = QuoteDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getUnderwriting().getAudit().getUpdatedBy())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getUnderwriting().getAssignee().getEmail())
                                        .setName(reply0.getUnderwriting().getRequester().getName())
                                        .setPhone(reply0.getUnderwriting().getRequester().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteDepreciation(outbound1);
        return GetDepreciationRulesResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(reply0.getUnderwriting().getRequester().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
