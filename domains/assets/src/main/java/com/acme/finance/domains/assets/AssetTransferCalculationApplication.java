package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferLine;
import com.acme.assets.v1.CalculateAssetTransferRequest;
import com.acme.assets.v1.CalculateAssetTransferResponse;
import com.acme.assets.v1.CalculateFixedAssetRequest;
import com.acme.assets.v1.CalculateFixedAssetResponse;
import com.acme.assets.v1.CompareAssetTransferOptionsRequest;
import com.acme.assets.v1.CompareAssetTransferOptionsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.EstimateAssetTransferRequest;
import com.acme.assets.v1.EstimateAssetTransferResponse;
import com.acme.assets.v1.ExplainAssetTransferCalculationRequest;
import com.acme.assets.v1.ExplainAssetTransferCalculationResponse;
import com.acme.assets.v1.ExplainDepreciationCalculationRequest;
import com.acme.assets.v1.ExplainDepreciationCalculationResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GetAssetTransferRatesRequest;
import com.acme.assets.v1.GetAssetTransferRatesResponse;
import com.acme.assets.v1.GetFixedAssetRatesRequest;
import com.acme.assets.v1.GetFixedAssetRatesResponse;
import com.acme.assets.v1.QuoteAssetTransferRequest;
import com.acme.assets.v1.QuoteAssetTransferResponse;
import com.acme.assets.v1.RecalculateAssetTransfersRequest;
import com.acme.assets.v1.RecalculateAssetTransfersResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.assets.v1.SimulateAssetTransferRequest;
import com.acme.assets.v1.SimulateAssetTransferResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.ExportAccessPoliciesRequest;
import com.acme.authorization.v1.ExportAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.BrowseComplianceCasesRequest;
import com.acme.compliance.v1.BrowseComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityRequest;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagDiscountAction;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.helpdesk.v1.ListTicketsRequest;
import com.acme.helpdesk.v1.ListTicketsResponse;
import com.acme.helpdesk.v1.TicketServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.ValidateRiskScenarioRequest;
import com.acme.risk.v1.ValidateRiskScenarioResponse;
import com.acme.seating.v1.RecalculateSeatMapsRequest;
import com.acme.seating.v1.RecalculateSeatMapsResponse;
import com.acme.seating.v1.SeatMap;
import com.acme.seating.v1.SeatMapCalculationServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of an asset transfer, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class AssetTransferCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(AssetTransferCalculationApplication.class);

    private final DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking;

    private final FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking;

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceBlockingStub complianceComplianceCaseQueryServiceBlocking;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final TicketServiceGrpc.TicketServiceBlockingStub helpdeskTicketServiceBlocking;

    private final AssetTransferCalculationRequestMapper mapper;

    private final AssetsRepository repository;

    private final RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking;

    private final RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking;

    private final Executor rpcExecutor;

    private final SeatMapCalculationServiceGrpc.SeatMapCalculationServiceBlockingStub seatingSeatMapCalculationServiceBlocking;

    public AssetTransferCalculationApplication(
            DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking,
            FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking,
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceBlockingStub complianceComplianceCaseQueryServiceBlocking,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            TicketServiceGrpc.TicketServiceBlockingStub helpdeskTicketServiceBlocking,
            AssetTransferCalculationRequestMapper mapper,
            AssetsRepository repository,
            RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking,
            RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SeatMapCalculationServiceGrpc.SeatMapCalculationServiceBlockingStub seatingSeatMapCalculationServiceBlocking) {
        this.assetsDepreciationCalculationServiceBlocking = assetsDepreciationCalculationServiceBlocking;
        this.assetsFixedAssetCalculationServiceBlocking = assetsFixedAssetCalculationServiceBlocking;
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseQueryServiceBlocking = complianceComplianceCaseQueryServiceBlocking;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.helpdeskTicketServiceBlocking = helpdeskTicketServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskRegisterValidationServiceBlocking = riskRiskRegisterValidationServiceBlocking;
        this.riskRiskScenarioValidationServiceBlocking = riskRiskScenarioValidationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.seatingSeatMapCalculationServiceBlocking = seatingSeatMapCalculationServiceBlocking;
    }

    /**
     * Calculates an asset transfer.
     */
    public CalculateAssetTransferResponse calculateAssetTransfer(CalculateAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = ExportAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAssetTransfer().getAudit().getUpdatedAt())
                        .setOwnerId(request.getAssetTransfer().getAssetTransferId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var stream0 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportAccessPolicies(outbound0);
        var reply0 = ExportAccessPoliciesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = ScheduleFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var channel1 = channels.lookup("acme.ledger.v1.FiscalPeriodExportService");
        var serviceName1 = "acme.ledger.v1" + "." + "FiscalPeriodExportService";
        var methodName1 = "Sch" + "eduleFiscalPeriodExport";
        var descriptor1 = MethodDescriptor.<ScheduleFiscalPeriodExportRequest, ScheduleFiscalPeriodExportResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ScheduleFiscalPeriodExportRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScheduleFiscalPeriodExportResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = CalculateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateFixedAssetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getAssetTransfer().getBillingAddress().getCountryCode())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .setRegion(request.getAssetTransfer().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateFixedAsset(value));
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateAssetTransfer", request.getAssetTransfer().getNumber());
        return CalculateAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .addLines(AssetTransferLine.newBuilder()
                                .setQuantity(reply2.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getQuantity())
                                .build())
                        .putAllMetadata(request.getAssetTransfer().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Estimates an asset transfer.
     */
    public EstimateAssetTransferResponse estimateAssetTransfer(EstimateAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = CheckRiskRegisterEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRecipient(request.getAssetTransfer().getBillingAddress().getRecipient())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRiskRegisterEligibility(value)).orElseThrow();

        var outbound1 = CalculateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateFixedAssetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getAssetTransfer().getBillingAddress().getLocality())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getRiskRegister().getLocation().getRecipient())
                                .build())
                        .build())
                .build();
        var reply1 = assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateFixedAsset(outbound1);

        List<CheckFeatureFlagEligibilityResponse> results2 = request.getAssetTransfer().getLinesList().stream()
                .map(element -> CheckFeatureFlagEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setDiscount(FeatureFlagDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkFeatureFlagEligibility(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? CheckFeatureFlagEligibilityResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateAssetTransfer", request.getAssetTransfer().getAssetTransferId());
        return EstimateAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRiskRegister().getAudit().getCreatedAt())
                                .setUpdatedAt(reply1.getFixedAsset().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply1.getFixedAsset().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(request.getAssetTransfer().getCounterparty())
                        .build())
                .build();
    }

    /**
     * Quotes an asset transfer.
     */
    public QuoteAssetTransferResponse quoteAssetTransfer(QuoteAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = ExplainDepreciationCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAssetTransfer().getAudit().getCreatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(request.getAssetTransfer().getBillingAddress().getLocality())
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .setRegion(request.getAssetTransfer().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainDepreciationCalculation(outbound0);

        var builder1 = RecalculateFixedAssetsRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFixedAsset(FixedAsset.newBuilder()
                .setAddress(PostalAddress.newBuilder()
                        .setPostalCode(reply0.getDepreciation().getLocation().getPostalCode())
                        .build())
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedAt(request.getAssetTransfer().getAudit().getUpdatedAt())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateFixedAssets(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteAssetTransfer", request.getAssetTransfer().getAssetTransferId());
        return QuoteAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .build())
                .setBreakdown(QuoteAssetTransferResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Compares an asset transfer.
     */
    public CompareAssetTransferOptionsResponse compareAssetTransferOptions(CompareAssetTransferOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareAssetTransferOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = CalculateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAssetTransfer().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateFixedAsset(outbound0), rpcExecutor);

        var outbound1 = GetFixedAssetRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAssetId(request.getAssetTransfer().getCounterparty().getFixedAssetId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getFixedAssetRates(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareAssetTransferOptions", request.getAssetTransfer().getAssetTransferId());
        return CompareAssetTransferOptionsResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setDisplayName(reply1.getFixedAsset().getOwner().getDisplayName())
                                .build())
                        .addLines(AssetTransferLine.newBuilder()
                                .setDescription(reply0.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getDescription())
                                .build())
                        .setStatus(request.getAssetTransfer().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalculates an asset transfer.
     */
    public RecalculateAssetTransfersResponse recalculateAssetTransfers(RecalculateAssetTransfersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateAssetTransfers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var builder0 = BrowseComplianceCasesRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setFilter(BrowseComplianceCasesRequest.Filter.newBuilder()
                .setCreatedAfter(request.getAssetTransfer().getAudit().getCreatedAt())
                .putAllLabels(request.getAnnotationsMap())
                .setRegion(request.getDetails().getDestination().getRegion())
                .build());
        var outbound0 = builder0.build();
        var reply0 = complianceComplianceCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseComplianceCases(outbound0);

        var outbound1 = mapper.recalculateAssetTransfersStage1(request);
        var reply1 = riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateRiskScenario(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateAssetTransfers", request.getAssetTransfer().getAssetTransferId());
        return RecalculateAssetTransfersResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .putAllMetadata(request.getAssetTransfer().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Simulates an asset transfer.
     */
    public SimulateAssetTransferResponse simulateAssetTransfer(SimulateAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = (ExplainFixedAssetCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAssetTransfer().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainFixedAssetCalculation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateAssetTransfer", request.getAssetTransfer().getAssetTransferId());
        return SimulateAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getFixedAsset().getAddress().getRegion())
                                .build())
                        .build())
                .setBreakdown(SimulateAssetTransferResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .build();
    }

    /**
     * Explains an asset transfer.
     */
    public ExplainAssetTransferCalculationResponse explainAssetTransferCalculation(ExplainAssetTransferCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainAssetTransferCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = (ResolveAuditTrailConflictRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getAssetTransfer().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ResolveAuditTrailConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build()).toBuilder().build();
        var reply0 = auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAuditTrailConflict(outbound0);

        var futures1 = request.getAssetTransfer().getLinesList().stream()
                .map(element -> ValidateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setDescription(element.getDescription())
                                .setQuantity(element.getQuantity())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateFixedAsset(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? ValidateFixedAssetResponse.getDefaultInstance()
                : results1.getLast();

        var results2 = new ArrayList<ListTicketsResponse>();
        for (var element : request.getAssetTransfer().getLinesList()) {
            ListTicketsRequest itemRequest = ListTicketsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListTicketsRequest.Filter.newBuilder()
                        .setOwnerId(element.getLineId())
                        .setRegion(reply1.getFixedAsset().getAddress().getRegion())
                        .build())
                .build();
            results2.add(helpdeskTicketServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listTickets(itemRequest));
        }
        var reply2 = results2.isEmpty() ? ListTicketsResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = BeanCopies.copy(CalculateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getAssetTransfer().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build(), CalculateFixedAssetRequest.class);
        var reply3 = assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateFixedAsset(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainAssetTransferCalculation", request.getAssetTransfer().getAssetTransferId());
        return ExplainAssetTransferCalculationResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply3.getFixedAsset().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(request.getAssetTransfer().getCounterparty())
                        .setTotal(reply0.getAuditTrail().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an asset transfer.
     */
    public GetAssetTransferRatesResponse getAssetTransferRates(GetAssetTransferRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAssetTransferRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "GetAssetTransferRates");

        var outbound0 = RecalculateSeatMapsRequest.newBuilder()
                .setContext(request.getContext())
                .setSeatMap(SeatMap.newBuilder()
                        .addChildren(SeatMap.newBuilder()
                                .addChildren(SeatMap.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getContext().getActor().getEmail())
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<RecalculateSeatMapsRequest, RecalculateSeatMapsResponse> port0 = RpcPort.adapt(seatingSeatMapCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateSeatMaps);
        var reply0 = port0.exchange(outbound0);
        return GetAssetTransferRatesResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getSeatMap().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
