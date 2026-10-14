package com.acme.finance.domains.billing;

import com.acme.billing.v1.CheckCreditNoteEligibilityRequest;
import com.acme.billing.v1.CheckCreditNoteEligibilityResponse;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.ExplainCreditNoteDecisionRequest;
import com.acme.billing.v1.ExplainCreditNoteDecisionResponse;
import com.acme.billing.v1.GetCreditNoteRulesRequest;
import com.acme.billing.v1.GetCreditNoteRulesResponse;
import com.acme.billing.v1.LintCreditNotesRequest;
import com.acme.billing.v1.LintCreditNotesResponse;
import com.acme.billing.v1.NormalizeCreditNoteRequest;
import com.acme.billing.v1.NormalizeCreditNoteResponse;
import com.acme.billing.v1.PreviewCreditNoteRequest;
import com.acme.billing.v1.PreviewCreditNoteResponse;
import com.acme.billing.v1.ScoreCreditNoteRequest;
import com.acme.billing.v1.ScoreCreditNoteResponse;
import com.acme.billing.v1.ValidateCreditNoteRequest;
import com.acme.billing.v1.ValidateCreditNoteResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Checks credit notes against the rules before anything acts on them.
 */
public abstract class AbstractCreditNoteValidationEndpoint extends CreditNoteValidationServiceGrpc.CreditNoteValidationServiceImplBase {
    protected final CreditNoteValidationApplication application;

    protected AbstractCreditNoteValidationEndpoint(CreditNoteValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a credit note.
     */
    @Override
    public void validateCreditNote(ValidateCreditNoteRequest request, StreamObserver<ValidateCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateCreditNote(request));
    }

    /**
     * Previews a credit note.
     */
    @Override
    public void previewCreditNote(PreviewCreditNoteRequest request, StreamObserver<PreviewCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewCreditNote(request));
    }

    /**
     * Checks a credit note.
     */
    @Override
    public void checkCreditNoteEligibility(CheckCreditNoteEligibilityRequest request, StreamObserver<CheckCreditNoteEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkCreditNoteEligibility(request));
    }

    /**
     * Normalizes a credit note.
     */
    @Override
    public void normalizeCreditNote(NormalizeCreditNoteRequest request, StreamObserver<NormalizeCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeCreditNote(request));
    }

    /**
     * Explains a credit note.
     */
    @Override
    public void explainCreditNoteDecision(ExplainCreditNoteDecisionRequest request, StreamObserver<ExplainCreditNoteDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainCreditNoteDecision(request));
    }

    /**
     * Scores a credit note.
     */
    @Override
    public void scoreCreditNote(ScoreCreditNoteRequest request, StreamObserver<ScoreCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreCreditNote(request));
    }

    /**
     * Lints a credit note.
     */
    @Override
    public void lintCreditNotes(LintCreditNotesRequest request, StreamObserver<LintCreditNotesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintCreditNotes(request));
    }

    /**
     * Returns a credit note.
     */
    @Override
    public void getCreditNoteRules(GetCreditNoteRulesRequest request, StreamObserver<GetCreditNoteRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCreditNoteRules(request));
    }
}
