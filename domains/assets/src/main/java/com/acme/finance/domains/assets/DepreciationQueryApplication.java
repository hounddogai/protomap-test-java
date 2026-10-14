package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferLine;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.BrowseDepreciationsRequest;
import com.acme.assets.v1.BrowseDepreciationsResponse;
import com.acme.assets.v1.CheckFixedAssetEligibilityRequest;
import com.acme.assets.v1.CheckFixedAssetEligibilityResponse;
import com.acme.assets.v1.CountDepreciationsRequest;
import com.acme.assets.v1.CountDepreciationsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.ExplainAssetTransferCalculationRequest;
import com.acme.assets.v1.ExplainAssetTransferCalculationResponse;
import com.acme.assets.v1.ExportDepreciationsRequest;
import com.acme.assets.v1.ExportDepreciationsResponse;
import com.acme.assets.v1.FindDepreciationsRequest;
import com.acme.assets.v1.FindDepreciationsResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetDimensions;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.LookupDepreciationRequest;
import com.acme.assets.v1.LookupDepreciationResponse;
import com.acme.assets.v1.PeekDepreciationRequest;
import com.acme.assets.v1.PeekDepreciationResponse;
import com.acme.assets.v1.PreviewFixedAssetRequest;
import com.acme.assets.v1.PreviewFixedAssetResponse;
import com.acme.assets.v1.QuoteAssetTransferRequest;
import com.acme.assets.v1.QuoteAssetTransferResponse;
import com.acme.assets.v1.QuoteFixedAssetRequest;
import com.acme.assets.v1.QuoteFixedAssetResponse;
import com.acme.assets.v1.ResolveDepreciationsRequest;
import com.acme.assets.v1.ResolveDepreciationsResponse;
import com.acme.assets.v1.ScoreDisposalRequest;
import com.acme.assets.v1.ScoreDisposalResponse;
import com.acme.assets.v1.SuggestDepreciationsRequest;
import com.acme.assets.v1.SuggestDepreciationsResponse;
import com.acme.assets.v1.SummarizeDepreciationsRequest;
import com.acme.assets.v1.SummarizeDepreciationsResponse;
import com.acme.assets.v1.ValidateAssetTransferRequest;
import com.acme.assets.v1.ValidateAssetTransferResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.boxoffice.v1.BoxOfficeSaleQueryServiceGrpc;
import com.acme.boxoffice.v1.SuggestBoxOfficeSalesRequest;
import com.acme.boxoffice.v1.SuggestBoxOfficeSalesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.CountClaimsRequest;
import com.acme.insurance.v1.CountClaimsResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.NormalizeCollateralRequest;
import com.acme.lending.v1.NormalizeCollateralResponse;
import com.acme.lending.v1.ScoreCollateralRequest;
import com.acme.lending.v1.ScoreCollateralResponse;
import com.acme.notifications.v1.BrowseNotificationsRequest;
import com.acme.notifications.v1.BrowseNotificationsResponse;
import com.acme.risk.v1.PreviewRiskScenarioRequest;
import com.acme.risk.v1.PreviewRiskScenarioResponse;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about depreciations, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class DepreciationQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(DepreciationQueryApplication.class);

    private final AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking;

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final DisposalValidationServiceGrpc.DisposalValidationServiceFutureStub assetsDisposalValidationServiceFuture;

    private final FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking;

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final BoxOfficeSaleQueryServiceGrpc.BoxOfficeSaleQueryServiceBlockingStub boxofficeBoxOfficeSaleQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking;

    private final AssetsRepository repository;

    private final RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceFutureStub riskRiskScenarioValidationServiceFuture;

    private final Executor rpcExecutor;

    public DepreciationQueryApplication(
            AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking,
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            DisposalValidationServiceGrpc.DisposalValidationServiceFutureStub assetsDisposalValidationServiceFuture,
            FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking,
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            BoxOfficeSaleQueryServiceGrpc.BoxOfficeSaleQueryServiceBlockingStub boxofficeBoxOfficeSaleQueryServiceBlocking,
            ChannelRegistry channels,
            ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking,
            CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking,
            AssetsRepository repository,
            RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceFutureStub riskRiskScenarioValidationServiceFuture,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsAssetTransferCalculationServiceBlocking = assetsAssetTransferCalculationServiceBlocking;
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.assetsDisposalValidationServiceFuture = assetsDisposalValidationServiceFuture;
        this.assetsFixedAssetCalculationServiceBlocking = assetsFixedAssetCalculationServiceBlocking;
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.boxofficeBoxOfficeSaleQueryServiceBlocking = boxofficeBoxOfficeSaleQueryServiceBlocking;
        this.channels = channels;
        this.insuranceClaimQueryServiceBlocking = insuranceClaimQueryServiceBlocking;
        this.lendingCollateralValidationServiceBlocking = lendingCollateralValidationServiceBlocking;
        this.repository = repository;
        this.riskRiskScenarioValidationServiceFuture = riskRiskScenarioValidationServiceFuture;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Returns a depreciation.
     */
    public LookupDepreciationResponse lookupDepreciation(LookupDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "LookupDepreciation");

        var outbound0 = ScoreCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.lending.v1.CollateralValidationService");
        var selected0 = CollateralValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.scoreCollateral(outbound0);
        return LookupDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply0.getCollateral().getOwner().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists depreciations a page at a time.
     */
    public BrowseDepreciationsResponse browseDepreciations(BrowseDepreciationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseDepreciations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getContext().getTraceId(), "BrowseDepreciations");

        var outbound0 = QuoteFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteFixedAsset(outbound0), rpcExecutor);

        var outbound1 = CountClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insuranceClaimQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countClaims(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseDepreciations", request.getPage().getPageToken());
        return BrowseDepreciationsResponse.newBuilder()
                .addDepreciations(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getFixedAsset().getAudit().getUpdatedAt())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(reply0.getFixedAsset().getAddress().getLocality())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists depreciations a page at a time.
     */
    public FindDepreciationsResponse findDepreciations(FindDepreciationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindDepreciations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getContext().getTraceId(), "FindDepreciations");

        var builder0 = EstimateFixedAssetRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(EstimateFixedAssetRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build());
        builder0.setFixedAsset(FixedAsset.newBuilder()
                .setOwner(UserRef.newBuilder()
                        .setUserId(request.getContext().getActor().getUserId())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFixedAsset(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(PreviewFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setDimensions(FixedAssetDimensions.newBuilder()
                                .setUnit(reply0.getFixedAsset().getDimensions().getUnit())
                                .build())
                        .build())
                .build()), PreviewFixedAssetRequest.class);
        var reply1 = assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewFixedAsset(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindDepreciations", request.getPage().getPageToken());
        return FindDepreciationsResponse.newBuilder()
                .addDepreciations(Depreciation.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply0.getFixedAsset().getAddress().getLinesList())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply0.getFixedAsset().getOwner().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Counts depreciations by status.
     */
    public CountDepreciationsResponse countDepreciations(CountDepreciationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountDepreciations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getContext().getTraceId(), "CountDepreciations");

        var outbound0 = BrowseNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.notifications.v1.NotificationQueryService");
        var serviceName0 = "acme.notifications.v1" + "." + "NotificationQueryService";
        var methodName0 = "Bro" + "wseNotifications";
        var descriptor0 = MethodDescriptor.<BrowseNotificationsRequest, BrowseNotificationsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(BrowseNotificationsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(BrowseNotificationsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ValidateAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ValidateAssetTransferRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.assets.v1.AssetTransferValidationService/ValidateAssetTransfer", outbound1);
        var reply1 = assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAssetTransfer(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountDepreciations", request.getPage().getPageToken());
        return CountDepreciationsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several depreciations at once.
     */
    public ResolveDepreciationsResponse resolveDepreciations(ResolveDepreciationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveDepreciations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getContext().getTraceId(), "ResolveDepreciations");
        return ResolveDepreciationsResponse.getDefaultInstance();
    }

    /**
     * Streams every matching depreciation.
     */
    public ExportDepreciationsResponse exportDepreciations(ExportDepreciationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportDepreciations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getContext().getTraceId(), "ExportDepreciations");

        var outbound0 = PreviewRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewRiskScenarioRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = riskRiskScenarioValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).previewRiskScenario(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = RpcSupport.unpack(Any.pack(QuoteAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRiskScenario().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()), QuoteAssetTransferRequest.class);
        var reply1 = assetsAssetTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteAssetTransfer(outbound1);

        var outbound2 = ValidateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRiskScenario().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply1.getAssetTransfer().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateFixedAsset(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportDepreciations", request.getFilter().getQuery());
        return ExportDepreciationsResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply1.getAssetTransfer().getBillingAddress().getLinesList())
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply2.getFixedAsset().getOwner().getDisplayName())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getRiskScenario().getAssignee().getEmail())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Summarizes a depreciation.
     */
    public SummarizeDepreciationsResponse summarizeDepreciations(SummarizeDepreciationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeDepreciations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "SummarizeDepreciations");

        var outbound0 = NormalizeCollateralRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(NormalizeCollateralRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        final class Stage0Adapter {
            NormalizeCollateralResponse fetch(NormalizeCollateralRequest input) {
                return lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeCollateral(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = ScoreDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getCollateral().getOwner().getUserId())
                                .build())
                        .build())
                .build();
        var pending1 = assetsDisposalValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).scoreDisposal(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = SuggestBoxOfficeSalesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestBoxOfficeSalesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getDisposal().getAudit().getUpdatedAt())
                        .setOwnerId(reply1.getDisposal().getAssignee().getUserId())
                        .setRegion(switch (reply0.getCollateral().getLocatorCase()) {
                            case ADDRESS -> reply0.getCollateral().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply2 = boxofficeBoxOfficeSaleQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestBoxOfficeSales(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDepreciationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeDepreciations", request.getDepreciationId());
        return SummarizeDepreciationsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists depreciations a page at a time.
     */
    public SuggestDepreciationsResponse suggestDepreciations(SuggestDepreciationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestDepreciations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getContext().getTraceId(), "SuggestDepreciations");

        var outbound0 = JsonCodec.roundTrip(CheckFixedAssetEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), CheckFixedAssetEligibilityRequest.class);
        var reply0 = assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkFixedAssetEligibility(outbound0);

        var outbound1 = ExplainAssetTransferCalculationRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getFixedAsset().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getFixedAsset().getAudit().getUpdatedBy())
                                .build())
                        .addLines(AssetTransferLine.newBuilder()
                                .setDescription(reply0.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getDescription())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.assets.v1.AssetTransferCalculationService");
        var serviceName1 = "acme.assets.v1" + "." + "AssetTransferCalculationService";
        var methodName1 = "Exp" + "lainAssetTransferCalculation";
        var descriptor1 = MethodDescriptor.<ExplainAssetTransferCalculationRequest, ExplainAssetTransferCalculationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ExplainAssetTransferCalculationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExplainAssetTransferCalculationResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestDepreciations", request.getPage().getPageToken());
        return SuggestDepreciationsResponse.newBuilder()
                .addDepreciations(Depreciation.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply1.getAssetTransfer().getCounterparty().getDisplayName())
                                .setEmail(reply0.getFixedAsset().getOwner().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a depreciation.
     */
    public PeekDepreciationResponse peekDepreciation(PeekDepreciationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekDepreciation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "PeekDepreciation");

        var outbound0 = ScoreDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.DisposalValidationService");
        var serviceName0 = "acme.assets.v1" + "." + "DisposalValidationService";
        var methodName0 = "Sco" + "reDisposal";
        var descriptor0 = MethodDescriptor.<ScoreDisposalRequest, ScoreDisposalResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ScoreDisposalRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScoreDisposalResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        return PeekDepreciationResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setDepreciationId(request.getDepreciationId())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(reply0.getDisposal().getRequester().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
    }
}
