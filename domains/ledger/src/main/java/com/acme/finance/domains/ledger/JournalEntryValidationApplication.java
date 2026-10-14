package com.acme.finance.domains.ledger;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.ValidateFeatureFlagRequest;
import com.acme.featureflags.v1.ValidateFeatureFlagResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.ExplainJournalEntryDecisionRequest;
import com.acme.ledger.v1.ExplainJournalEntryDecisionResponse;
import com.acme.ledger.v1.GetJournalEntryRulesRequest;
import com.acme.ledger.v1.GetJournalEntryRulesResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.ledger.v1.PreviewJournalEntryRequest;
import com.acme.ledger.v1.PreviewJournalEntryResponse;
import com.acme.ledger.v1.ValidateJournalEntryRequest;
import com.acme.ledger.v1.ValidateJournalEntryResponse;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks journal entries against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class JournalEntryValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(JournalEntryValidationApplication.class);

    private final ChannelRegistry channels;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final LedgerRepository repository;

    public JournalEntryValidationApplication(
            ChannelRegistry channels,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            LedgerRepository repository) {
        this.channels = channels;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Validates a journal entry.
     */
    public ValidateJournalEntryResponse validateJournalEntry(ValidateJournalEntryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateJournalEntry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveJournalEntry(request.getContext().getTenantId(), request.getJournalEntry());
        log.atDebug().addKeyValue("auditValue", request.getJournalEntry().getJournalEntryId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateJournalEntry", request.getJournalEntry().getJournalEntryId());
        return ValidateJournalEntryResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .setNotes(request.getJournalEntry().getNotes())
                        .build())
                .build();
    }

    /**
     * Previews a journal entry.
     */
    public PreviewJournalEntryResponse previewJournalEntry(PreviewJournalEntryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewJournalEntry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveJournalEntry(request.getContext().getTenantId(), request.getJournalEntry());
        log.atDebug().addKeyValue("auditValue", request.getJournalEntry().getJournalEntryId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewJournalEntry", request.getJournalEntry().getJournalEntryId());
        return PreviewJournalEntryResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .putAllMetadata(request.getJournalEntry().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Normalizes a journal entry.
     */
    public NormalizeJournalEntryResponse normalizeJournalEntry(NormalizeJournalEntryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeJournalEntry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveJournalEntry(request.getContext().getTenantId(), request.getJournalEntry());

        var outbound0 = NormalizeLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setCard(LedgerAccountCardFunding.newBuilder()
                                .setToken(request.getJournalEntry().getCard().getToken())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.ledger.v1.LedgerAccountValidationService");
        var selected0 = LedgerAccountValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.normalizeLedgerAccount(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getJournalEntry().getJournalEntryId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeJournalEntry", request.getJournalEntry().getJournalEntryId());
        return NormalizeJournalEntryResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .putAllMetadata(request.getJournalEntry().getMetadataMap())
                        .build())
                .setOutcome(NormalizeJournalEntryResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a journal entry.
     */
    public ExplainJournalEntryDecisionResponse explainJournalEntryDecision(ExplainJournalEntryDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainJournalEntryDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveJournalEntry(request.getContext().getTenantId(), request.getJournalEntry());

        var outbound0 = ValidateFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateFeatureFlagRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getJournalEntry().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getJournalEntry().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getJournalEntry().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        RpcPort<ValidateFeatureFlagRequest, ValidateFeatureFlagResponse> port0 = RpcPort.adapt(featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::validateFeatureFlag);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getJournalEntry().getJournalEntryId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainJournalEntryDecision", request.getJournalEntry().getJournalEntryId());
        return ExplainJournalEntryDecisionResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setCounterparty(LedgerAccountRef.newBuilder()
                                .setDisplayName(reply0.getFeatureFlag().getOwner().getDisplayName())
                                .build())
                        .setIssuedAt(request.getJournalEntry().getIssuedAt())
                        .setJournalEntryId(request.getJournalEntryId())
                        .build())
                .setOutcome(ExplainJournalEntryDecisionResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints a journal entry.
     */
    public LintJournalEntriesResponse lintJournalEntries(LintJournalEntriesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintJournalEntries")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveJournalEntry(request.getContext().getTenantId(), request.getJournalEntry());
        log.atDebug().addKeyValue("auditValue", request.getJournalEntry().getJournalEntryId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintJournalEntries", request.getJournalEntry().getJournalEntryId());
        return LintJournalEntriesResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .setTotal(request.getJournalEntry().getTotal())
                        .build())
                .build();
    }

    /**
     * Returns a journal entry.
     */
    public GetJournalEntryRulesResponse getJournalEntryRules(GetJournalEntryRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetJournalEntryRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "JournalEntry", request.getJournalEntryId(), "GetJournalEntryRules");
        return GetJournalEntryRulesResponse.newBuilder()
                .setJournalEntry(JournalEntry.newBuilder()
                        .setJournalEntryId(request.getJournalEntryId())
                        .build())
                .build();
    }
}
