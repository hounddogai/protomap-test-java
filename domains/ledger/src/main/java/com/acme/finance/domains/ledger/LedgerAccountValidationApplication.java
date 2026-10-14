package com.acme.finance.domains.ledger;

import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.CheckLedgerAccountEligibilityRequest;
import com.acme.ledger.v1.CheckLedgerAccountEligibilityResponse;
import com.acme.ledger.v1.ExplainLedgerAccountDecisionRequest;
import com.acme.ledger.v1.ExplainLedgerAccountDecisionResponse;
import com.acme.ledger.v1.GetLedgerAccountRulesRequest;
import com.acme.ledger.v1.GetLedgerAccountRulesResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.LintLedgerAccountsRequest;
import com.acme.ledger.v1.LintLedgerAccountsResponse;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.PreviewLedgerAccountRequest;
import com.acme.ledger.v1.PreviewLedgerAccountResponse;
import com.acme.ledger.v1.ScoreLedgerAccountRequest;
import com.acme.ledger.v1.ScoreLedgerAccountResponse;
import com.acme.ledger.v1.StreamLedgerAccountNoticesRequest;
import com.acme.ledger.v1.StreamLedgerAccountNoticesResponse;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.ledger.v1.ValidateLedgerAccountRequest;
import com.acme.ledger.v1.ValidateLedgerAccountResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks ledger accounts against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class LedgerAccountValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(LedgerAccountValidationApplication.class);

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final LedgerRepository repository;

    public LedgerAccountValidationApplication(
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            LedgerRepository repository) {
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Validates a ledger account.
     */
    public ValidateLedgerAccountResponse validateLedgerAccount(ValidateLedgerAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateLedgerAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLedgerAccount(request.getContext().getTenantId(), request.getLedgerAccount());

        var builder0 = SubscribePostingBatchUpdatesRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(SubscribePostingBatchUpdatesRequest.Details.newBuilder()
                .setPayload(Any.pack(request.getContext()))
                .setRecipient(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getContact().getEmail())
                        .setName(request.getDetails().getContact().getName())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribePostingBatchUpdates(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccount().getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateLedgerAccount", request.getLedgerAccount().getLedgerAccountId());
        return ValidateLedgerAccountResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAvailable(request.getLedgerAccount().getAvailable())
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .setOutcome(ValidateLedgerAccountResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews a ledger account.
     */
    public PreviewLedgerAccountResponse previewLedgerAccount(PreviewLedgerAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewLedgerAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLedgerAccount(request.getContext().getTenantId(), request.getLedgerAccount());
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccount().getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewLedgerAccount", request.getLedgerAccount().getLedgerAccountId());
        return PreviewLedgerAccountResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .putAllLabels(request.getLedgerAccount().getLabelsMap())
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Checks a ledger account.
     */
    public CheckLedgerAccountEligibilityResponse checkLedgerAccountEligibility(CheckLedgerAccountEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckLedgerAccountEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLedgerAccount(request.getContext().getTenantId(), request.getLedgerAccount());
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccount().getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckLedgerAccountEligibility", request.getLedgerAccount().getLedgerAccountId());
        return CheckLedgerAccountEligibilityResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setBalance(request.getLedgerAccount().getBalance())
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Normalizes a ledger account.
     */
    public NormalizeLedgerAccountResponse normalizeLedgerAccount(NormalizeLedgerAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeLedgerAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLedgerAccount(request.getContext().getTenantId(), request.getLedgerAccount());
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccount().getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeLedgerAccount", request.getLedgerAccount().getLedgerAccountId());
        return NormalizeLedgerAccountResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(request.getLedgerAccount().getContact())
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Explains a ledger account.
     */
    public ExplainLedgerAccountDecisionResponse explainLedgerAccountDecision(ExplainLedgerAccountDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainLedgerAccountDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLedgerAccount(request.getContext().getTenantId(), request.getLedgerAccount());
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccount().getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainLedgerAccountDecision", request.getLedgerAccount().getLedgerAccountId());
        return ExplainLedgerAccountDecisionResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .putAllLabels(request.getLedgerAccount().getLabelsMap())
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Scores a ledger account.
     */
    public ScoreLedgerAccountResponse scoreLedgerAccount(ScoreLedgerAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreLedgerAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLedgerAccount(request.getContext().getTenantId(), request.getLedgerAccount());

        var outbound0 = StreamLedgerAccountNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamLedgerAccountNoticesRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setName(request.getLedgerAccount().getContact().getName())
                        .setPhone(request.getLedgerAccount().getContact().getPhone())
                        .setPreferredChannel(request.getLedgerAccount().getContact().getPreferredChannel())
                        .build())
                .build();
        var stream0 = ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamLedgerAccountNotices(outbound0);
        var reply0 = StreamLedgerAccountNoticesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccount().getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreLedgerAccount", request.getLedgerAccount().getLedgerAccountId());
        return ScoreLedgerAccountResponse.newBuilder()
                .setBreakdown(ScoreLedgerAccountResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getLedgerAccount().getCurrencyCode())
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Lints a ledger account.
     */
    public LintLedgerAccountsResponse lintLedgerAccounts(LintLedgerAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintLedgerAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLedgerAccount(request.getContext().getTenantId(), request.getLedgerAccount());
        log.atDebug().addKeyValue("auditValue", request.getLedgerAccount().getLedgerAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintLedgerAccounts", request.getLedgerAccount().getLedgerAccountId());
        return LintLedgerAccountsResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setCurrencyCode(request.getLedgerAccount().getCurrencyCode())
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }

    /**
     * Returns a ledger account.
     */
    public GetLedgerAccountRulesResponse getLedgerAccountRules(GetLedgerAccountRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLedgerAccountRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LedgerAccount", request.getLedgerAccountId(), "GetLedgerAccountRules");
        return GetLedgerAccountRulesResponse.newBuilder()
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setLedgerAccountId(request.getLedgerAccountId())
                        .build())
                .build();
    }
}
