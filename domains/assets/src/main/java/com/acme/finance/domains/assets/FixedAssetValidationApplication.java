package com.acme.finance.domains.assets;

import com.acme.assets.v1.CalculateFixedAssetRequest;
import com.acme.assets.v1.CalculateFixedAssetResponse;
import com.acme.assets.v1.CheckFixedAssetEligibilityRequest;
import com.acme.assets.v1.CheckFixedAssetEligibilityResponse;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetDimensions;
import com.acme.assets.v1.GetFixedAssetRatesRequest;
import com.acme.assets.v1.GetFixedAssetRatesResponse;
import com.acme.assets.v1.GetFixedAssetRulesRequest;
import com.acme.assets.v1.GetFixedAssetRulesResponse;
import com.acme.assets.v1.LintFixedAssetsRequest;
import com.acme.assets.v1.LintFixedAssetsResponse;
import com.acme.assets.v1.PreviewFixedAssetRequest;
import com.acme.assets.v1.PreviewFixedAssetResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.assets.v1.ScoreFixedAssetRequest;
import com.acme.assets.v1.ScoreFixedAssetResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.PreviewSanctionRequest;
import com.acme.compliance.v1.PreviewSanctionResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.devices.v1.ManagedDevice;
import com.acme.devices.v1.ManagedDeviceCalculationServiceGrpc;
import com.acme.devices.v1.SimulateManagedDeviceRequest;
import com.acme.devices.v1.SimulateManagedDeviceResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersRequest;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersResponse;
import com.acme.lending.v1.RecalculateRepaymentsRequest;
import com.acme.lending.v1.RecalculateRepaymentsResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.risk.v1.ExplainStressTestDecisionRequest;
import com.acme.risk.v1.ExplainStressTestDecisionResponse;
import com.acme.risk.v1.ResolveRiskRegistersRequest;
import com.acme.risk.v1.ResolveRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks fixed assets against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class FixedAssetValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(FixedAssetValidationApplication.class);

    private final FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking;

    private final ManagedDeviceCalculationServiceGrpc.ManagedDeviceCalculationServiceBlockingStub devicesManagedDeviceCalculationServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking;

    private final FixedAssetValidationRequestMapper mapper;

    private final AssetsRepository repository;

    private final RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceBlockingStub riskRiskRegisterQueryServiceBlocking;

    private final StressTestValidationServiceGrpc.StressTestValidationServiceBlockingStub riskStressTestValidationServiceBlocking;

    public FixedAssetValidationApplication(
            FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            ChannelRegistry channels,
            SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking,
            ManagedDeviceCalculationServiceGrpc.ManagedDeviceCalculationServiceBlockingStub devicesManagedDeviceCalculationServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking,
            FixedAssetValidationRequestMapper mapper,
            AssetsRepository repository,
            RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceBlockingStub riskRiskRegisterQueryServiceBlocking,
            StressTestValidationServiceGrpc.StressTestValidationServiceBlockingStub riskStressTestValidationServiceBlocking) {
        this.assetsFixedAssetCalculationServiceBlocking = assetsFixedAssetCalculationServiceBlocking;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.channels = channels;
        this.complianceSanctionValidationServiceBlocking = complianceSanctionValidationServiceBlocking;
        this.devicesManagedDeviceCalculationServiceBlocking = devicesManagedDeviceCalculationServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.lendingRepaymentCalculationServiceBlocking = lendingRepaymentCalculationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskRegisterQueryServiceBlocking = riskRiskRegisterQueryServiceBlocking;
        this.riskStressTestValidationServiceBlocking = riskStressTestValidationServiceBlocking;
    }

    /**
     * Validates a fixed asset.
     */
    public ValidateFixedAssetResponse validateFixedAsset(ValidateFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = CalculateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setDimensions(FixedAssetDimensions.newBuilder()
                                .setUnit(request.getFixedAsset().getDimensions().getUnit())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getFixedAsset().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.FixedAssetCalculationService");
        var selected0 = FixedAssetCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.calculateFixedAsset(outbound0);

        var outbound1 = ExplainStressTestDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainStressTestDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getFixedAsset().getAddress().getCountryCode())
                                .build())
                        .addLines(StressTestLine.newBuilder()
                                .setQuantity(reply0.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getQuantity())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainStressTestDecisionRequest, ExplainStressTestDecisionResponse> port1 = RpcPort.adapt(riskStressTestValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainStressTestDecision);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateFixedAsset", request.getFixedAsset().getFixedAssetId());
        return ValidateFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getStressTest().getAudit().getUpdatedAt())
                                .build())
                        .setCode(request.getFixedAsset().getCode())
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setComponentId(reply0.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getComponentId())
                                .build())
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews a fixed asset.
     */
    public PreviewFixedAssetResponse previewFixedAsset(PreviewFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = (RecalculateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setQuantity(request.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getQuantity())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateFixedAssets(outbound0);

        var outbound1 = GetFixedAssetRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAssetId(request.getFixedAsset().getFixedAssetId())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getFixedAssetRates(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewFixedAsset", request.getFixedAsset().getCode());
        return PreviewFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getFixedAsset().getAudit().getUpdatedBy())
                                .build())
                        .setCapacity(request.getFixedAsset().getCapacity())
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Checks a fixed asset.
     */
    public CheckFixedAssetEligibilityResponse checkFixedAssetEligibility(CheckFixedAssetEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckFixedAssetEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = RpcSupport.unpack(Any.pack(EstimateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getFixedAsset().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build()), EstimateFixedAssetRequest.class);
        var reply0 = assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFixedAsset(outbound0);

        var outbound1 = ScoreMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFixedAsset().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getFixedAsset().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(switch (request.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> request.getFixedAsset().getAddress().getCountryCode();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreMandate(outbound1);

        var builder2 = NotifyLedgerAccountWatchersRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setDetails(NotifyLedgerAccountWatchersRequest.Details.newBuilder()
                .setPayload(Any.pack(request.getContext()))
                .setRecipient(ContactInfo.newBuilder()
                        .setEmail(reply1.getMandate().getCounterparty().getEmail())
                        .setName(request.getDetails().getContact().getName())
                        .setPhone(request.getDetails().getContact().getPhone())
                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                        .build())
                .build());
        var outbound2 = builder2.build();
        var reply2 = ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyLedgerAccountWatchers(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckFixedAssetEligibility", request.getFixedAsset().getFixedAssetId());
        return CheckFixedAssetEligibilityResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .setName(request.getFixedAsset().getName())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getFixedAsset().getOwner().getUserId())
                                .build())
                        .putAllSpecs(reply0.getFixedAsset().getSpecsMap())
                        .build())
                .setOutcome(CheckFixedAssetEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Scores a fixed asset.
     */
    public ScoreFixedAssetResponse scoreFixedAsset(ScoreFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = mapper.scoreFixedAssetStage0(request);
        var reply0 = devicesManagedDeviceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateManagedDevice(outbound0);

        var builder1 = ResolveRiskRegistersRequest.newBuilder();
        builder1.setContext(request.getContext());
        var outbound1 = builder1.build();
        var reply1 = riskRiskRegisterQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveRiskRegisters(outbound1);

        var outbound2 = ExplainFixedAssetCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFixedAsset().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.assets.v1.FixedAssetCalculationService");
        var selected2 = FixedAssetCalculationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.explainFixedAssetCalculation(outbound2);

        var outbound3 = PreviewSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(reply2.getFixedAsset().getAddress().getLinesList())
                                                .setLocality(switch (reply2.getFixedAsset().getLocatorCase()) {
                                                    case ADDRESS -> reply2.getFixedAsset().getAddress().getLocality();
                                                    default -> "";
                                                })
                                                .setPostalCode(switch (reply0.getManagedDevice().getLocatorCase()) {
                                                    case ADDRESS -> reply0.getManagedDevice().getAddress().getPostalCode();
                                                    default -> "";
                                                })
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply0.getManagedDevice().getOwner().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply3 = complianceSanctionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewSanction(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreFixedAsset", request.getFixedAsset().getFixedAssetId());
        return ScoreFixedAssetResponse.newBuilder()
                .setBreakdown(ScoreFixedAssetResponse.Breakdown.newBuilder()
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply3.getSanction().getAudit().getEtag())
                                .build())
                        .setCode(request.getFixedAsset().getCode())
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setComponentId(reply2.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getComponentId())
                                .build())
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lints a fixed asset.
     */
    public LintFixedAssetsResponse lintFixedAssets(LintFixedAssetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintFixedAssets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = RecalculateRepaymentsRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(switch (request.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> request.getFixedAsset().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getContact().getName())
                                        .setPhone(request.getDetails().getContact().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = lendingRepaymentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateRepayments(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintFixedAssets", request.getFixedAsset().getFixedAssetId());
        return LintFixedAssetsResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setCapacity(request.getFixedAsset().getCapacity())
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .setOutcome(LintFixedAssetsResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a fixed asset.
     */
    public GetFixedAssetRulesResponse getFixedAssetRules(GetFixedAssetRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFixedAssetRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "GetFixedAssetRules");

        var outbound0 = EstimateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFixedAsset(outbound0);
        return GetFixedAssetRulesResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .putComponents("primary", FixedAssetComponent.newBuilder()
                                .setComponentId(reply0.getFixedAsset().getComponentsOrDefault("primary", FixedAssetComponent.getDefaultInstance()).getComponentId())
                                .build())
                        .setDimensions(FixedAssetDimensions.newBuilder()
                                .setHeight(reply0.getFixedAsset().getDimensions().getHeight())
                                .build())
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .build();
    }
}
