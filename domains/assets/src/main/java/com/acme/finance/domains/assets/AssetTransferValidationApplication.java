package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.CalculateDepreciationRequest;
import com.acme.assets.v1.CalculateDepreciationResponse;
import com.acme.assets.v1.CheckAssetTransferEligibilityRequest;
import com.acme.assets.v1.CheckAssetTransferEligibilityResponse;
import com.acme.assets.v1.CompareAssetTransferOptionsRequest;
import com.acme.assets.v1.CompareAssetTransferOptionsResponse;
import com.acme.assets.v1.CompareDisposalOptionsRequest;
import com.acme.assets.v1.CompareDisposalOptionsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.ExplainAssetTransferCalculationRequest;
import com.acme.assets.v1.ExplainAssetTransferCalculationResponse;
import com.acme.assets.v1.ExplainAssetTransferDecisionRequest;
import com.acme.assets.v1.ExplainAssetTransferDecisionResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.GetAssetTransferRulesRequest;
import com.acme.assets.v1.GetAssetTransferRulesResponse;
import com.acme.assets.v1.LintAssetTransfersRequest;
import com.acme.assets.v1.LintAssetTransfersResponse;
import com.acme.assets.v1.NormalizeAssetTransferRequest;
import com.acme.assets.v1.NormalizeAssetTransferResponse;
import com.acme.assets.v1.PreviewAssetTransferRequest;
import com.acme.assets.v1.PreviewAssetTransferResponse;
import com.acme.assets.v1.RecalculateDepreciationsRequest;
import com.acme.assets.v1.RecalculateDepreciationsResponse;
import com.acme.assets.v1.ValidateAssetTransferRequest;
import com.acme.assets.v1.ValidateAssetTransferResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.ValidateAuditEventRequest;
import com.acme.audit.v1.ValidateAuditEventResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.WatchCollectionCasesRequest;
import com.acme.collections.v1.WatchCollectionCasesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.CompareAttestationOptionsRequest;
import com.acme.compliance.v1.CompareAttestationOptionsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.ValidateSanctionRequest;
import com.acme.compliance.v1.ValidateSanctionResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.StreamLedgerAccountNoticesRequest;
import com.acme.ledger.v1.StreamLedgerAccountNoticesResponse;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.lending.v1.SummarizeLoansRequest;
import com.acme.lending.v1.SummarizeLoansResponse;
import com.acme.risk.v1.CheckStressTestEligibilityRequest;
import com.acme.risk.v1.CheckStressTestEligibilityResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestLineTax;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
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
 * Checks asset transfers against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class AssetTransferValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(AssetTransferValidationApplication.class);

    private final AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking;

    private final DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking;

    private final DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking;

    private final FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking;

    private final AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking;

    private final SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final LoanQueryServiceGrpc.LoanQueryServiceBlockingStub lendingLoanQueryServiceBlocking;

    private final AssetTransferValidationRequestMapper mapper;

    private final AssetsRepository repository;

    private final StressTestValidationServiceGrpc.StressTestValidationServiceBlockingStub riskStressTestValidationServiceBlocking;

    private final Executor rpcExecutor;

    private final VenueValidationServiceGrpc.VenueValidationServiceFutureStub venuesVenueValidationServiceFuture;

    public AssetTransferValidationApplication(
            AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking,
            DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking,
            DisposalCalculationServiceGrpc.DisposalCalculationServiceBlockingStub assetsDisposalCalculationServiceBlocking,
            FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking,
            AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking,
            SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            LoanQueryServiceGrpc.LoanQueryServiceBlockingStub lendingLoanQueryServiceBlocking,
            AssetTransferValidationRequestMapper mapper,
            AssetsRepository repository,
            StressTestValidationServiceGrpc.StressTestValidationServiceBlockingStub riskStressTestValidationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            VenueValidationServiceGrpc.VenueValidationServiceFutureStub venuesVenueValidationServiceFuture) {
        this.assetsAssetTransferCalculationServiceBlocking = assetsAssetTransferCalculationServiceBlocking;
        this.assetsDepreciationCalculationServiceBlocking = assetsDepreciationCalculationServiceBlocking;
        this.assetsDisposalCalculationServiceBlocking = assetsDisposalCalculationServiceBlocking;
        this.assetsFixedAssetCalculationServiceBlocking = assetsFixedAssetCalculationServiceBlocking;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseServiceBlocking = collectionsCollectionCaseServiceBlocking;
        this.complianceAttestationCalculationServiceBlocking = complianceAttestationCalculationServiceBlocking;
        this.complianceSanctionValidationServiceBlocking = complianceSanctionValidationServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.lendingLoanQueryServiceBlocking = lendingLoanQueryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskStressTestValidationServiceBlocking = riskStressTestValidationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.venuesVenueValidationServiceFuture = venuesVenueValidationServiceFuture;
    }

    /**
     * Validates an asset transfer.
     */
    public ValidateAssetTransferResponse validateAssetTransfer(ValidateAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = CompareAttestationOptionsRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAssetTransfer().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = complianceAttestationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAttestationOptions(outbound0);

        var outbound1 = CompareAssetTransferOptionsRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getAttestation().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> assetsAssetTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAssetTransferOptions(value));
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateAssetTransfer", request.getAssetTransfer().getAssetTransferId());
        return ValidateAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getAssetTransfer().getAudit().getUpdatedBy())
                                .build())
                        .setDueAt(request.getAssetTransfer().getDueAt())
                        .setIssuedAt(reply0.getAttestation().getIssuedAt())
                        .build())
                .setOutcome(ValidateAssetTransferResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Previews an asset transfer.
     */
    public PreviewAssetTransferResponse previewAssetTransfer(PreviewAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = ValidateVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getAssetTransfer().getBillingAddress().getPostalCode())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAssetTransfer().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var pending0 = venuesVenueValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).validateVenue(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewAssetTransfer", request.getAssetTransfer().getAssetTransferId());
        return PreviewAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getVenue().getAddress().getLocality())
                                .build())
                        .setDueAt(request.getAssetTransfer().getDueAt())
                        .build())
                .setOutcome(PreviewAssetTransferResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Checks an asset transfer.
     */
    public CheckAssetTransferEligibilityResponse checkAssetTransferEligibility(CheckAssetTransferEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckAssetTransferEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = RecalculateDepreciationsRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getAssetTransfer().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setDetails(RecalculateDepreciationsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getAssetTransfer().getBillingAddress().getLocality())
                                .setPostalCode(request.getAssetTransfer().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.DepreciationCalculationService");
        var selected0 = DepreciationCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.recalculateDepreciations(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckAssetTransferEligibility", request.getAssetTransfer().getAssetTransferId());
        return CheckAssetTransferEligibilityResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getDepreciation().getAudit().getEtag())
                                .build())
                        .setBillingAddress(request.getAssetTransfer().getBillingAddress())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Normalizes an asset transfer.
     */
    public NormalizeAssetTransferResponse normalizeAssetTransfer(NormalizeAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = CompareDisposalOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .addComments(DisposalComment.newBuilder()
                                .setCreatedAt(request.getAssetTransfer().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsDisposalCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareDisposalOptions(outbound0), rpcExecutor);

        var outbound1 = EstimateFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateFixedAssetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getAssetTransfer().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFixedAsset(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        List<CheckStressTestEligibilityResponse> results2 = request.getAssetTransfer().getLinesList().stream()
                .map(element -> CheckStressTestEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setStressTest(StressTest.newBuilder()
                        .addLines(StressTestLine.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .setTax(StressTestLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(riskStressTestValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkStressTestEligibility(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? CheckStressTestEligibilityResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = ValidateAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(switch (reply0.getDisposal().getOriginCase()) {
                                    case CUSTOMER -> reply0.getDisposal().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ValidateAuditEventRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getDisposal().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var reply3 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAuditEvent(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeAssetTransfer", request.getAssetTransfer().getAssetTransferId());
        return NormalizeAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getFixedAsset().getAudit().getEtag())
                                .setUpdatedAt(reply0.getDisposal().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getFixedAsset().getAddress().getRegion())
                                .build())
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setFixedAssetId(reply0.getDisposal().getCustomer().getFixedAssetId())
                                .build())
                        .setNotes(request.getAssetTransfer().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Explains an asset transfer.
     */
    public ExplainAssetTransferDecisionResponse explainAssetTransferDecision(ExplainAssetTransferDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainAssetTransferDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = RpcSupport.unpack(Any.pack(ExplainAssetTransferCalculationRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setFixedAssetId(request.getAssetTransfer().getCounterparty().getFixedAssetId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainAssetTransferCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .build()), ExplainAssetTransferCalculationRequest.class);
        var reply0 = assetsAssetTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainAssetTransferCalculation(outbound0);

        var outbound1 = WatchCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchCollectionCasesRequest.Filter.newBuilder()
                        .setRegion(reply0.getAssetTransfer().getBillingAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getContact().getEmail())
                        .setPhone(request.getDetails().getContact().getPhone())
                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                        .build())
                .build();
        var stream1 = collectionsCollectionCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchCollectionCases(outbound1);
        var reply1 = WatchCollectionCasesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainAssetTransferDecision", request.getAssetTransfer().getAssetTransferId());
        return ExplainAssetTransferDecisionResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setIssuedAt(request.getAssetTransfer().getIssuedAt())
                        .setStatus(reply0.getAssetTransfer().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints an asset transfer.
     */
    public LintAssetTransfersResponse lintAssetTransfers(LintAssetTransfersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintAssetTransfers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = StreamLedgerAccountNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                        .build())
                .build();
        var stream0 = ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamLedgerAccountNotices(outbound0);
        var reply0 = StreamLedgerAccountNoticesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = mapper.lintAssetTransfersStage1(request);
        var reply1 = assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateDepreciation(outbound1);

        var outbound2 = (ValidateSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setLocality(request.getAssetTransfer().getBillingAddress().getLocality())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = complianceSanctionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSanction(outbound2);

        var outbound3 = SummarizeLoansRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        RpcPort<SummarizeLoansRequest, SummarizeLoansResponse> port3 = RpcPort.adapt(lendingLoanQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::summarizeLoans);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintAssetTransfers", request.getAssetTransfer().getAssetTransferId());
        return LintAssetTransfersResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getDepreciation().getLocation().getLocality())
                                .build())
                        .setNotes(request.getAssetTransfer().getNotes())
                        .build())
                .setOutcome(LintAssetTransfersResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns an asset transfer.
     */
    public GetAssetTransferRulesResponse getAssetTransferRules(GetAssetTransferRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAssetTransferRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "GetAssetTransferRules");
        return GetAssetTransferRulesResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .build())
                .build();
    }
}
