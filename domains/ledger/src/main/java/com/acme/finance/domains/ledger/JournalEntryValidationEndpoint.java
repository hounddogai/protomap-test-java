package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.ExplainJournalEntryDecisionRequest;
import com.acme.ledger.v1.ExplainJournalEntryDecisionResponse;
import com.acme.ledger.v1.GetJournalEntryRulesRequest;
import com.acme.ledger.v1.GetJournalEntryRulesResponse;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.ledger.v1.PreviewJournalEntryRequest;
import com.acme.ledger.v1.PreviewJournalEntryResponse;
import com.acme.ledger.v1.ValidateJournalEntryRequest;
import com.acme.ledger.v1.ValidateJournalEntryResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks journal entries against the rules before anything acts on them.
 */
@GrpcService
public class JournalEntryValidationEndpoint extends JournalEntryValidationServiceGrpc.JournalEntryValidationServiceImplBase {
    protected final JournalEntryValidationApplication application;

    public JournalEntryValidationEndpoint(JournalEntryValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a journal entry.
     */
    @Override
    public void validateJournalEntry(ValidateJournalEntryRequest request, StreamObserver<ValidateJournalEntryResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateJournalEntry(request));
    }

    /**
     * Previews a journal entry.
     */
    @Override
    public void previewJournalEntry(PreviewJournalEntryRequest request, StreamObserver<PreviewJournalEntryResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewJournalEntry(request));
    }

    /**
     * Normalizes a journal entry.
     */
    @Override
    public void normalizeJournalEntry(NormalizeJournalEntryRequest request, StreamObserver<NormalizeJournalEntryResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeJournalEntry(request));
    }

    /**
     * Explains a journal entry.
     */
    @Override
    public void explainJournalEntryDecision(ExplainJournalEntryDecisionRequest request, StreamObserver<ExplainJournalEntryDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainJournalEntryDecision(request));
    }

    /**
     * Lints a journal entry.
     */
    @Override
    public void lintJournalEntries(LintJournalEntriesRequest request, StreamObserver<LintJournalEntriesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintJournalEntries(request));
    }

    /**
     * Returns a journal entry.
     */
    @Override
    public void getJournalEntryRules(GetJournalEntryRulesRequest request, StreamObserver<GetJournalEntryRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getJournalEntryRules(request));
    }
}
