package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationLine;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlDiscountAction;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.compliance.v1.ExplainAttestationDecisionRequest;
import com.acme.compliance.v1.ExplainAttestationDecisionResponse;
import com.acme.compliance.v1.GetAttestationRulesRequest;
import com.acme.compliance.v1.GetAttestationRulesResponse;
import com.acme.compliance.v1.LintAttestationsRequest;
import com.acme.compliance.v1.LintAttestationsResponse;
import com.acme.compliance.v1.NormalizeAttestationRequest;
import com.acme.compliance.v1.NormalizeAttestationResponse;
import com.acme.compliance.v1.PreviewAttestationRequest;
import com.acme.compliance.v1.PreviewAttestationResponse;
import com.acme.compliance.v1.PreviewControlRequest;
import com.acme.compliance.v1.PreviewControlResponse;
import com.acme.compliance.v1.QuoteControlRequest;
import com.acme.compliance.v1.QuoteControlResponse;
import com.acme.compliance.v1.SimulateControlRequest;
import com.acme.compliance.v1.SimulateControlResponse;
import com.acme.compliance.v1.ValidateAttestationRequest;
import com.acme.compliance.v1.ValidateAttestationResponse;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.featureflags.v1.ListFeatureFlagsRequest;
import com.acme.featureflags.v1.ListFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryBankSettlement;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.lending.v1.CloneLoanApplicationRequest;
import com.acme.lending.v1.CloneLoanApplicationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationServiceGrpc;
import com.google.protobuf.Any;
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
 * Checks attestations against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class AttestationValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(AttestationValidationApplication.class);

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking;

    private final LoanApplicationServiceGrpc.LoanApplicationServiceBlockingStub lendingLoanApplicationServiceBlocking;

    private final ComplianceRepository repository;

    private final Executor rpcExecutor;

    public AttestationValidationApplication(
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            ChannelRegistry channels,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking,
            FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking,
            LoanApplicationServiceGrpc.LoanApplicationServiceBlockingStub lendingLoanApplicationServiceBlocking,
            ComplianceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.channels = channels;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.complianceControlValidationServiceBlocking = complianceControlValidationServiceBlocking;
        this.featureflagsFeatureFlagServiceBlocking = featureflagsFeatureFlagServiceBlocking;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.ledgerJournalEntryValidationServiceBlocking = ledgerJournalEntryValidationServiceBlocking;
        this.lendingLoanApplicationServiceBlocking = lendingLoanApplicationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Validates an attestation.
     */
    public ValidateAttestationResponse validateAttestation(ValidateAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = NormalizeJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setBank(JournalEntryBankSettlement.newBuilder()
                                .setBic(switch (request.getAttestation().getSettlementCase()) {
                                    case BANK -> request.getAttestation().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeJournalEntry(outbound0);

        var outbound1 = (ScoreAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreAuditEventRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getAttestation().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreAuditEvent(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateAttestation", request.getAttestation().getAttestationId());
        return ValidateAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getJournalEntry().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(request.getAttestation().getBillingAddress())
                        .build())
                .setOutcome(ValidateAttestationResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Previews an attestation.
     */
    public PreviewAttestationResponse previewAttestation(PreviewAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = EstimateControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getAttestation().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<EstimateControlRequest, EstimateControlResponse>() {
            @Override
            public EstimateControlResponse apply(EstimateControlRequest input) {
                return complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateControl(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = SimulateControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getControl().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getControl().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateControl(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewAttestation", request.getAttestation().getAttestationId());
        return PreviewAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getControl().getAudit().getUpdatedBy())
                                .build())
                        .setNumber(request.getAttestation().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Normalizes an attestation.
     */
    public NormalizeAttestationResponse normalizeAttestation(NormalizeAttestationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeAttestation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = CloneLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getAttestation().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var reply0 = lendingLoanApplicationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneLoanApplication(outbound0);

        List<ListFeatureFlagsResponse> results1 = request.getAttestation().getLinesList().stream()
                .map(element -> ListFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFeatureFlagsRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .setRegion(reply0.getLoanApplication().getBillingAddress().getRegion())
                        .build())
                .build())
                .flatMap(value -> Stream.of(featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listFeatureFlags(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? ListFeatureFlagsResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeAttestation", request.getAttestation().getAttestationId());
        return NormalizeAttestationResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getLoanApplication().getBillingAddress().getLinesList())
                                .setPostalCode(reply0.getLoanApplication().getBillingAddress().getPostalCode())
                                .build())
                        .setNumber(request.getAttestation().getNumber())
                        .build())
                .build();
    }

    /**
     * Explains an attestation.
     */
    public ExplainAttestationDecisionResponse explainAttestationDecision(ExplainAttestationDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainAttestationDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = PreviewControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAttestation().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = complianceControlValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewControl(outbound0);

        var futures1 = request.getAttestation().getLinesList().stream()
                .map(element -> QuoteCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getControl().getAudit().getCreatedBy())
                                .setEtag(reply0.getControl().getAudit().getEtag())
                                .build())
                        .setDescription(element.getDescription())
                        .setDiscount(CoverageDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCoverage(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? QuoteCoverageResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainAttestationDecision", request.getAttestation().getAttestationId());
        return ExplainAttestationDecisionResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setStatus(request.getAttestation().getStatus())
                        .build())
                .setOutcome(ExplainAttestationDecisionResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Lints an attestation.
     */
    public LintAttestationsResponse lintAttestations(LintAttestationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintAttestations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAttestation(request.getContext().getTenantId(), request.getAttestation());

        var outbound0 = SimulateControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAttestation().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setDetails(SimulateControlRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getAttestation().getBillingAddress().getLocality())
                                .setRegion(request.getAttestation().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.compliance.v1.ControlCalculationService");
        var selected0 = ControlCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.simulateControl(outbound0);

        var outbound1 = QuoteControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getControl().getAudit().getCreatedAt())
                                .setEtag(request.getAttestation().getAudit().getEtag())
                                .build())
                        .setDiscount(ControlDiscountAction.newBuilder()
                                .setPercent(reply0.getControl().getDiscount().getPercent())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteControl(value));
        log.atDebug().addKeyValue("auditValue", request.getAttestation().getAttestationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintAttestations", request.getAttestation().getAttestationId());
        return LintAttestationsResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .setCounterparty(ComplianceCaseRef.newBuilder()
                                .setDisplayName(reply0.getControl().getOwner().getDisplayName())
                                .build())
                        .addLines(AttestationLine.newBuilder()
                                .setDescription(reply0.getControl().getDescription())
                                .build())
                        .setNumber(request.getAttestation().getNumber())
                        .build())
                .build();
    }

    /**
     * Returns an attestation.
     */
    public GetAttestationRulesResponse getAttestationRules(GetAttestationRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAttestationRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Attestation", request.getAttestationId(), "GetAttestationRules");
        return GetAttestationRulesResponse.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setAttestationId(request.getAttestationId())
                        .build())
                .build();
    }
}
