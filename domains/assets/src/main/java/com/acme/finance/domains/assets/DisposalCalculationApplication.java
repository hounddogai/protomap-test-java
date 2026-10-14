package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferLine;
import com.acme.assets.v1.CalculateDepreciationRequest;
import com.acme.assets.v1.CalculateDepreciationResponse;
import com.acme.assets.v1.CalculateDisposalRequest;
import com.acme.assets.v1.CalculateDisposalResponse;
import com.acme.assets.v1.CheckDepreciationEligibilityRequest;
import com.acme.assets.v1.CheckDepreciationEligibilityResponse;
import com.acme.assets.v1.CompareDepreciationOptionsRequest;
import com.acme.assets.v1.CompareDepreciationOptionsResponse;
import com.acme.assets.v1.CompareDisposalOptionsRequest;
import com.acme.assets.v1.CompareDisposalOptionsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.DepreciationWeeklyRule;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.EstimateDepreciationRequest;
import com.acme.assets.v1.EstimateDepreciationResponse;
import com.acme.assets.v1.EstimateDisposalRequest;
import com.acme.assets.v1.EstimateDisposalResponse;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.ExplainDepreciationCalculationRequest;
import com.acme.assets.v1.ExplainDepreciationCalculationResponse;
import com.acme.assets.v1.ExplainDisposalCalculationRequest;
import com.acme.assets.v1.ExplainDisposalCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GetDisposalRatesRequest;
import com.acme.assets.v1.GetDisposalRatesResponse;
import com.acme.assets.v1.QuoteDepreciationRequest;
import com.acme.assets.v1.QuoteDepreciationResponse;
import com.acme.assets.v1.QuoteDisposalRequest;
import com.acme.assets.v1.QuoteDisposalResponse;
import com.acme.assets.v1.RecalculateAssetTransfersRequest;
import com.acme.assets.v1.RecalculateAssetTransfersResponse;
import com.acme.assets.v1.RecalculateDisposalsRequest;
import com.acme.assets.v1.RecalculateDisposalsResponse;
import com.acme.assets.v1.ScoreDepreciationRequest;
import com.acme.assets.v1.ScoreDepreciationResponse;
import com.acme.assets.v1.ScoreFixedAssetRequest;
import com.acme.assets.v1.ScoreFixedAssetResponse;
import com.acme.assets.v1.SimulateDisposalRequest;
import com.acme.assets.v1.SimulateDisposalResponse;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.ExportFeatureFlagsRequest;
import com.acme.featureflags.v1.ExportFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.NormalizePremiumRequest;
import com.acme.insurance.v1.NormalizePremiumResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.ExplainRiskRegisterDecisionRequest;
import com.acme.risk.v1.ExplainRiskRegisterDecisionResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a disposal, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class DisposalCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(DisposalCalculationApplication.class);

    private final AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking;

    private final DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking;

    private final DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking;

    private final FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking;

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking;

    private final PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final DisposalCalculationRequestMapper mapper;

    private final AssetsRepository repository;

    private final RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking;

    private final Executor rpcExecutor;

    public DisposalCalculationApplication(
            AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking,
            DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking,
            DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking,
            FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking,
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking,
            PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            DisposalCalculationRequestMapper mapper,
            AssetsRepository repository,
            RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsAssetTransferCalculationServiceBlocking = assetsAssetTransferCalculationServiceBlocking;
        this.assetsDepreciationCalculationServiceBlocking = assetsDepreciationCalculationServiceBlocking;
        this.assetsDepreciationValidationServiceBlocking = assetsDepreciationValidationServiceBlocking;
        this.assetsFixedAssetCalculationServiceBlocking = assetsFixedAssetCalculationServiceBlocking;
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.featureflagsFeatureFlagQueryServiceBlocking = featureflagsFeatureFlagQueryServiceBlocking;
        this.insurancePremiumValidationServiceBlocking = insurancePremiumValidationServiceBlocking;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskRegisterValidationServiceBlocking = riskRiskRegisterValidationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Calculates a disposal.
     */
    public CalculateDisposalResponse calculateDisposal(CalculateDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = CheckDepreciationEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(request.getDisposal().getRequester().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsDepreciationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkDepreciationEligibility(outbound0), rpcExecutor);

        var outbound1 = ExplainDepreciationCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(request.getDisposal().getRequester().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainDepreciationCalculation(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var builder2 = CalculateDepreciationRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setDepreciation(Depreciation.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedBy(reply1.getDepreciation().getAudit().getCreatedBy())
                        .build())
                .addParticipants(DepreciationParticipant.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDisposal().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build());
        builder2.setDetails(CalculateDepreciationRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .addAllLines(request.getDetails().getDestination().getLinesList())
                        .build())
                .build());
        var outbound2 = builder2.build();
        var reply2 = assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateDepreciation(outbound2);

        var builder3 = EstimateFixedAssetRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setFixedAsset(FixedAsset.newBuilder()
                .setOwner(UserRef.newBuilder()
                        .setDisplayName(reply0.getDepreciation().getOrganizer().getDisplayName())
                        .build())
                .build());
        var outbound3 = builder3.build();
        var reply3 = assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFixedAsset(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateDisposal", request.getDisposal().getDisposalId());
        return CalculateDisposalResponse.newBuilder()
                .setBreakdown(CalculateDisposalResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply3.getBreakdown().getCurrencyCode())
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setDisposalId(request.getDisposalId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getDepreciation().getOrganizer().getEmail())
                                .build())
                        .setStatus(request.getDisposal().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Estimates a disposal.
     */
    public EstimateDisposalResponse estimateDisposal(EstimateDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = BeanCopies.copy(QuoteDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getDisposal().getAssignee().getEmail())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDisposal().getRequester().getName())
                                        .setPhone(request.getDisposal().getRequester().getPhone())
                                        .build())
                                .build())
                        .build())
                .setDetails(QuoteDepreciationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .build(), QuoteDepreciationRequest.class);
        var reply0 = assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteDepreciation(outbound0);

        var outbound1 = BeanCopies.copy(ScoreDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(reply0.getDepreciation().getLocation().getLocality())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDisposal().getRequester().getEmail())
                                        .build())
                                .build())
                        .setWeekly(DepreciationWeeklyRule.newBuilder()
                                .setInterval(reply0.getDepreciation().getDaily().getInterval())
                                .build())
                        .build())
                .setDetails(ScoreDepreciationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getDepreciation().getLocation().getPostalCode())
                                .build())
                        .build())
                .build(), ScoreDepreciationRequest.class);
        var reply1 = assetsDepreciationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreDepreciation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateDisposal", request.getDisposal().getDisposalId());
        return EstimateDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply1.getDepreciation().getOrganizer().getUserId())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getDepreciation().getOrganizer().getEmail())
                                .build())
                        .setSubject(request.getDisposal().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Quotes a disposal.
     */
    public QuoteDisposalResponse quoteDisposal(QuoteDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = JsonCodec.roundTrip(CompareDepreciationOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(request.getDisposal().getRequester().getPhone())
                                        .build())
                                .build())
                        .build())
                .build(), CompareDepreciationOptionsRequest.class);
        var reply0 = assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareDepreciationOptions(outbound0);

        var outbound1 = StreamAuditTrailExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditTrailExportRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getDepreciation().getAudit().getCreatedAt())
                        .setRegion(reply0.getDepreciation().getLocation().getRegion())
                        .build())
                .build();
        var stream1 = auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditTrailExport(outbound1);
        var reply1 = StreamAuditTrailExportResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteDisposal", request.getDisposal().getDisposalId());
        return QuoteDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getDepreciation().getAudit().getCreatedBy())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .setSubject(request.getDisposal().getSubject())
                        .build())
                .build();
    }

    /**
     * Compares a disposal.
     */
    public CompareDisposalOptionsResponse compareDisposalOptions(CompareDisposalOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareDisposalOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = EstimateDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getDisposal().getAudit().getUpdatedAt())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDisposal().getRequester().getName())
                                        .build())
                                .build())
                        .build())
                .setDetails(EstimateDepreciationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateDepreciation(value));

        var outbound1 = ExportFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportFeatureFlagsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDisposal().getAudit().getCreatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var stream1 = featureflagsFeatureFlagQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportFeatureFlags(outbound1);
        var reply1 = ExportFeatureFlagsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareDisposalOptions", request.getDisposal().getDisposalId());
        return CompareDisposalOptionsResponse.newBuilder()
                .setBreakdown(CompareDisposalOptionsResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .addComments(DisposalComment.newBuilder()
                                .setCreatedAt(reply0.getDepreciation().getAudit().getCreatedAt())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Recalculates a disposal.
     */
    public RecalculateDisposalsResponse recalculateDisposals(RecalculateDisposalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateDisposals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = ScoreFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreFixedAssetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(switch (request.getDisposal().getOriginCase()) {
                            case CUSTOMER -> request.getDisposal().getCustomer().getFixedAssetId();
                            default -> "";
                        })
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDisposal().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreFixedAsset(outbound0);

        var outbound1 = CompareFiscalPeriodVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareFiscalPeriodVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRecipient(switch (reply0.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> reply0.getFixedAsset().getAddress().getRecipient();
                                    default -> "";
                                })
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .addParticipants(FiscalPeriodParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDisposal().getAssignee().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareFiscalPeriodVersions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateDisposals", request.getDisposal().getDisposalId());
        return RecalculateDisposalsResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getFiscalPeriod().getOrganizer().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getFixedAsset().getAudit().getUpdatedAt())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .setSubject(request.getDisposal().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Simulates a disposal.
     */
    public SimulateDisposalResponse simulateDisposal(SimulateDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = QuoteDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDisposal().getRequester().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteDepreciation(outbound0), rpcExecutor);

        var outbound1 = CheckRiskRegisterEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedBy(request.getDisposal().getAudit().getUpdatedBy())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDisposal().getAssignee().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRiskRegisterEligibility(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var results2 = new ArrayList<ExplainRiskRegisterDecisionResponse>();
        for (var element : request.getDisposal().getCommentsList()) {
            ExplainRiskRegisterDecisionRequest itemRequest = ExplainRiskRegisterDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(element.getCreatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(element.getAuthorEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
            results2.add(riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainRiskRegisterDecision(itemRequest));
        }
        var reply2 = results2.isEmpty() ? ExplainRiskRegisterDecisionResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = CompareDepreciationOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getRiskRegister().getAudit().getUpdatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(reply2.getRiskRegister().getLocation().getRegion())
                                .build())
                        .build())
                .build();
        var reply3 = assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareDepreciationOptions(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateDisposal", request.getDisposal().getDisposalId());
        return SimulateDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getDepreciation().getAudit().getCreatedBy())
                                .setUpdatedAt(reply1.getRiskRegister().getAudit().getUpdatedAt())
                                .build())
                        .putAllCustomFields(request.getDisposal().getCustomFieldsMap())
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a disposal.
     */
    public ExplainDisposalCalculationResponse explainDisposalCalculation(ExplainDisposalCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainDisposalCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = NormalizePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizePremiumRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDisposal().getAssignee().getEmail())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getDisposal().getAudit().getEtag())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(request.getDisposal().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<NormalizePremiumRequest, NormalizePremiumResponse>() {
            @Override
            public NormalizePremiumResponse apply(NormalizePremiumRequest input) {
                return insurancePremiumValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePremium(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var results1 = new ArrayList<CompareDepreciationOptionsResponse>();
        for (var element : request.getDisposal().getCommentsList()) {
            CompareDepreciationOptionsRequest itemRequest = CompareDepreciationOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(element.getAuthorEmail())
                                        .setName(request.getDetails().getContact().getName())
                                        .build())
                                .build())
                        .build())
                .build();
            results1.add(assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareDepreciationOptions(itemRequest));
        }
        var reply1 = results1.isEmpty() ? CompareDepreciationOptionsResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainDisposalCalculation", request.getDisposal().getDisposalId());
        return ExplainDisposalCalculationResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(request.getDisposal().getAssignee())
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setOutcome(ExplainDisposalCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a disposal.
     */
    public GetDisposalRatesResponse getDisposalRates(GetDisposalRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDisposalRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "GetDisposalRates");

        var outbound0 = mapper.getDisposalRatesStage0(request);
        var reply0 = assetsAssetTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateAssetTransfers(outbound0);
        return GetDisposalRatesResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
