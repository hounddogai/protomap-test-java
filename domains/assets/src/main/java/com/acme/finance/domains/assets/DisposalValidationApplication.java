package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.CheckAssetTransferEligibilityRequest;
import com.acme.assets.v1.CheckAssetTransferEligibilityResponse;
import com.acme.assets.v1.CompareDepreciationOptionsRequest;
import com.acme.assets.v1.CompareDepreciationOptionsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GetDisposalRatesRequest;
import com.acme.assets.v1.GetDisposalRatesResponse;
import com.acme.assets.v1.GetDisposalRulesRequest;
import com.acme.assets.v1.GetDisposalRulesResponse;
import com.acme.assets.v1.LintAssetTransfersRequest;
import com.acme.assets.v1.LintAssetTransfersResponse;
import com.acme.assets.v1.LintDisposalsRequest;
import com.acme.assets.v1.LintDisposalsResponse;
import com.acme.assets.v1.NormalizeDisposalRequest;
import com.acme.assets.v1.NormalizeDisposalResponse;
import com.acme.assets.v1.PreviewDisposalRequest;
import com.acme.assets.v1.PreviewDisposalResponse;
import com.acme.assets.v1.QuoteDepreciationRequest;
import com.acme.assets.v1.QuoteDepreciationResponse;
import com.acme.assets.v1.ScoreDisposalRequest;
import com.acme.assets.v1.ScoreDisposalResponse;
import com.acme.assets.v1.ValidateDisposalRequest;
import com.acme.assets.v1.ValidateDisposalResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.audit.v1.AnnotateAuditEventVersionRequest;
import com.acme.audit.v1.AnnotateAuditEventVersionResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.AttestationLine;
import com.acme.compliance.v1.RecalculateAttestationsRequest;
import com.acme.compliance.v1.RecalculateAttestationsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralDimensions;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Checks disposals against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class DisposalValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(DisposalValidationApplication.class);

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking;

    private final FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking;

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking;

    private final CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking;

    private final AssetsRepository repository;

    private final Executor rpcExecutor;

    public DisposalValidationApplication(
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking,
            FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking,
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            ChannelRegistry channels,
            AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking,
            CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking,
            AssetsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.assetsDepreciationCalculationServiceBlocking = assetsDepreciationCalculationServiceBlocking;
        this.assetsFixedAssetCalculationServiceBlocking = assetsFixedAssetCalculationServiceBlocking;
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.channels = channels;
        this.complianceAttestationCalculationServiceBlocking = complianceAttestationCalculationServiceBlocking;
        this.lendingCollateralCalculationServiceBlocking = lendingCollateralCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Validates a disposal.
     */
    public ValidateDisposalResponse validateDisposal(ValidateDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = SubscribeAuditEventUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeAuditEventUpdatesRequest.Details.newBuilder()
                        .setChannel(request.getDisposal().getChannel().getChannel())
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDisposal().getAssignee().getEmail())
                                .setPhone(request.getDisposal().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeAuditEventUpdates(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateDisposal", request.getDisposal().getDisposalId());
        return ValidateDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(request.getDisposal().getAssignee())
                        .setDisposalId(request.getDisposalId())
                        .build())
                .setOutcome(ValidateDisposalResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Previews a disposal.
     */
    public PreviewDisposalResponse previewDisposal(PreviewDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = GetDisposalRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposalId(request.getDisposal().getDisposalId())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.DisposalCalculationService");
        var serviceName0 = "acme.assets.v1" + "." + "DisposalCalculationService";
        var methodName0 = "Get" + "DisposalRates";
        var descriptor0 = MethodDescriptor.<GetDisposalRatesRequest, GetDisposalRatesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(GetDisposalRatesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(GetDisposalRatesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = CompareDepreciationOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDisposal().getRequester().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareDepreciationOptions(value));
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewDisposal", request.getDisposal().getSubject());
        return PreviewDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getDepreciation().getAudit().getCreatedAt())
                                .setEtag(reply0.getDisposal().getAudit().getEtag())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .setRequester(request.getDisposal().getRequester())
                        .build())
                .build();
    }

    /**
     * Normalizes a disposal.
     */
    public NormalizeDisposalResponse normalizeDisposal(NormalizeDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = EstimateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getDisposal().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        Function<EstimateFixedAssetRequest, EstimateFixedAssetResponse> operation0 = assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::estimateFixedAsset;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = AnnotateAuditEventVersionRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(switch (request.getDisposal().getOriginCase()) {
                                    case CUSTOMER -> request.getDisposal().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(AnnotateAuditEventVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply1 = auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateAuditEventVersion(outbound1);

        var outbound2 = SimulateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setDimensions(CollateralDimensions.newBuilder()
                                .setWidth(reply0.getFixedAsset().getDimensions().getWidth())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCollateral(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeDisposal", request.getDisposal().getDisposalId());
        return NormalizeDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .addComments(DisposalComment.newBuilder()
                                .setCreatedAt(reply2.getCollateral().getAudit().getCreatedAt())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .setPriority(request.getDisposal().getPriority())
                        .build())
                .setOutcome(NormalizeDisposalResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getAuditEvent().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Scores a disposal.
     */
    public ScoreDisposalResponse scoreDisposal(ScoreDisposalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreDisposal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var outbound0 = QuoteDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDisposal().getAudit().getCreatedAt())
                                .setEtag(request.getDisposal().getAudit().getEtag())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .setDetails(QuoteDepreciationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.assets.v1.DepreciationCalculationService/QuoteDepreciation", outbound0);
        var reply0 = assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteDepreciation(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreDisposal", request.getDisposal().getDisposalId());
        return ScoreDisposalResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getDepreciation().getAudit().getCreatedBy())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .setStatus(request.getDisposal().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints a disposal.
     */
    public LintDisposalsResponse lintDisposals(LintDisposalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintDisposals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDisposal(request.getContext().getTenantId(), request.getDisposal());

        var futures0 = request.getDisposal().getCommentsList().stream()
                .map(element -> ValidateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateFixedAssetRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDisposal().getAssignee().getDisplayName())
                                .setEmail(request.getDisposal().getAssignee().getEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateFixedAsset(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ValidateFixedAssetResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = LintAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setFixedAssetId(switch (request.getDisposal().getOriginCase()) {
                                    case CUSTOMER -> request.getDisposal().getCustomer().getFixedAssetId();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAssetTransfers(outbound1);

        List<CheckAssetTransferEligibilityResponse> results2 = request.getDisposal().getCommentsList().stream()
                .map(element -> CheckAssetTransferEligibilityRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkAssetTransferEligibility(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? CheckAssetTransferEligibilityResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getDisposal().getDisposalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintDisposals", request.getDisposal().getDisposalId());
        return LintDisposalsResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(request.getDisposal().getAssignee())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getAssetTransfer().getAudit().getCreatedBy())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .build())
                .build();
    }

    /**
     * Returns a disposal.
     */
    public GetDisposalRulesResponse getDisposalRules(GetDisposalRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDisposalRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Disposal", request.getDisposalId(), "GetDisposalRules");

        var outbound0 = RecalculateAttestationsRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setIssuedAt(request.getContext().getDeadline())
                        .addLines(AttestationLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = complianceAttestationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateAttestations(outbound0);
        return GetDisposalRulesResponse.newBuilder()
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAttestation().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getAttestation().getAudit().getCreatedBy())
                                .build())
                        .setDisposalId(request.getDisposalId())
                        .build())
                .build();
    }
}
