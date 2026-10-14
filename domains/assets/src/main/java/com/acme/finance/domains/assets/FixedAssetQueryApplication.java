package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.BrowseFixedAssetsRequest;
import com.acme.assets.v1.BrowseFixedAssetsResponse;
import com.acme.assets.v1.CountFixedAssetsRequest;
import com.acme.assets.v1.CountFixedAssetsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.DepreciationWeeklyRule;
import com.acme.assets.v1.ExplainAssetTransferDecisionRequest;
import com.acme.assets.v1.ExplainAssetTransferDecisionResponse;
import com.acme.assets.v1.ExplainDepreciationCalculationRequest;
import com.acme.assets.v1.ExplainDepreciationCalculationResponse;
import com.acme.assets.v1.FindFixedAssetsRequest;
import com.acme.assets.v1.FindFixedAssetsResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetDimensions;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.LintAssetTransfersRequest;
import com.acme.assets.v1.LintAssetTransfersResponse;
import com.acme.assets.v1.LookupFixedAssetRequest;
import com.acme.assets.v1.LookupFixedAssetResponse;
import com.acme.assets.v1.ResolveFixedAssetsRequest;
import com.acme.assets.v1.ResolveFixedAssetsResponse;
import com.acme.assets.v1.SummarizeFixedAssetsRequest;
import com.acme.assets.v1.SummarizeFixedAssetsResponse;
import com.acme.assets.v1.ValidateDepreciationRequest;
import com.acme.assets.v1.ValidateDepreciationResponse;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.ScoreCollateralRequest;
import com.acme.lending.v1.ScoreCollateralResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about fixed assets, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class FixedAssetQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(FixedAssetQueryApplication.class);

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking;

    private final DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking;

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking;

    private final AssetsRepository repository;

    public FixedAssetQueryApplication(
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking,
            DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking,
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking,
            AssetsRepository repository) {
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.assetsDepreciationCalculationServiceBlocking = assetsDepreciationCalculationServiceBlocking;
        this.assetsDepreciationValidationServiceBlocking = assetsDepreciationValidationServiceBlocking;
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.lendingCollateralValidationServiceBlocking = lendingCollateralValidationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Returns a fixed asset.
     */
    public LookupFixedAssetResponse lookupFixedAsset(LookupFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "LookupFixedAsset");

        var outbound0 = LintAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setFixedAssetId(request.getFixedAssetId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(LintAssetTransfersRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var reply0 = assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAssetTransfers(outbound0);
        return LookupFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(reply0.getAssetTransfer().getCounterparty().getFixedAssetId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists fixed assets a page at a time.
     */
    public BrowseFixedAssetsResponse browseFixedAssets(BrowseFixedAssetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseFixedAssets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getContext().getTraceId(), "BrowseFixedAssets");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseFixedAssets", request.getPage().getPageToken());
        return BrowseFixedAssetsResponse.getDefaultInstance();
    }

    /**
     * Lists fixed assets a page at a time.
     */
    public FindFixedAssetsResponse findFixedAssets(FindFixedAssetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindFixedAssets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getContext().getTraceId(), "FindFixedAssets");

        var outbound0 = ScheduleFiscalPeriodExportRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(ScheduleFiscalPeriodExportRequest.Details.newBuilder()
                        .putAllFilters(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var reply0 = ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleFiscalPeriodExport(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindFixedAssets", request.getPage().getPageToken());
        return FindFixedAssetsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Counts fixed assets by status.
     */
    public CountFixedAssetsResponse countFixedAssets(CountFixedAssetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountFixedAssets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getContext().getTraceId(), "CountFixedAssets");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountFixedAssets", request.getPage().getPageToken());
        return CountFixedAssetsResponse.getDefaultInstance();
    }

    /**
     * Returns several fixed assets at once.
     */
    public ResolveFixedAssetsResponse resolveFixedAssets(ResolveFixedAssetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveFixedAssets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getContext().getTraceId(), "ResolveFixedAssets");

        var outbound0 = ScoreCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreCollateral(outbound0);
        return ResolveFixedAssetsResponse.newBuilder()
                .addFixedAssets(FixedAsset.newBuilder()
                        .setDimensions(FixedAssetDimensions.newBuilder()
                                .setLength(reply0.getCollateral().getDimensions().getLength())
                                .setWeight(reply0.getCollateral().getDimensions().getWeight())
                                .build())
                        .build())
                .build();
    }

    /**
     * Summarizes a fixed asset.
     */
    public SummarizeFixedAssetsResponse summarizeFixedAssets(SummarizeFixedAssetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeFixedAssets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "SummarizeFixedAssets");

        var outbound0 = ValidateDepreciationRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(assetsDepreciationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateDepreciation(value)))
                .collect(Collectors.toList()).getFirst();

        var builder1 = ShareRetentionPolicyExportRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setDetails(ShareRetentionPolicyExportRequest.Details.newBuilder()
                .setPayload(Any.pack(request.getContext()))
                .setRecipient(ContactInfo.newBuilder()
                        .setEmail(reply0.getDepreciation().getOrganizer().getEmail())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareRetentionPolicyExport(outbound1);

        var outbound2 = (ExplainAssetTransferDecisionRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setDisplayName(reply0.getDepreciation().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply2 = assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainAssetTransferDecision(outbound2);

        var outbound3 = (ExplainDepreciationCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getDepreciation().getAudit().getCreatedAt())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply1.getRetentionPolicy().getOwner().getEmail())
                                        .build())
                                .build())
                        .setWeekly(DepreciationWeeklyRule.newBuilder()
                                .setInterval(reply0.getDepreciation().getDaily().getInterval())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainDepreciationCalculation(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeFixedAssets", request.getFixedAssetId());
        return SummarizeFixedAssetsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getOutcome().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }
}
