package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.CheckLedgerAccountEligibilityRequest;
import com.acme.ledger.v1.CheckLedgerAccountEligibilityResponse;
import com.acme.ledger.v1.ExplainLedgerAccountDecisionRequest;
import com.acme.ledger.v1.ExplainLedgerAccountDecisionResponse;
import com.acme.ledger.v1.GetLedgerAccountRulesRequest;
import com.acme.ledger.v1.GetLedgerAccountRulesResponse;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.LintLedgerAccountsRequest;
import com.acme.ledger.v1.LintLedgerAccountsResponse;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.ledger.v1.PreviewLedgerAccountRequest;
import com.acme.ledger.v1.PreviewLedgerAccountResponse;
import com.acme.ledger.v1.ScoreLedgerAccountRequest;
import com.acme.ledger.v1.ScoreLedgerAccountResponse;
import com.acme.ledger.v1.ValidateLedgerAccountRequest;
import com.acme.ledger.v1.ValidateLedgerAccountResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks ledger accounts against the rules before anything acts on them.
 */
@GrpcService
public class LedgerAccountValidationEndpoint extends LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceImplBase {
    protected final LedgerAccountValidationApplication application;

    public LedgerAccountValidationEndpoint(LedgerAccountValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a ledger account.
     */
    @Override
    public void validateLedgerAccount(ValidateLedgerAccountRequest request, StreamObserver<ValidateLedgerAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateLedgerAccount(request));
    }

    /**
     * Previews a ledger account.
     */
    @Override
    public void previewLedgerAccount(PreviewLedgerAccountRequest request, StreamObserver<PreviewLedgerAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewLedgerAccount(request));
    }

    /**
     * Checks a ledger account.
     */
    @Override
    public void checkLedgerAccountEligibility(CheckLedgerAccountEligibilityRequest request, StreamObserver<CheckLedgerAccountEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkLedgerAccountEligibility(request));
    }

    /**
     * Normalizes a ledger account.
     */
    @Override
    public void normalizeLedgerAccount(NormalizeLedgerAccountRequest request, StreamObserver<NormalizeLedgerAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeLedgerAccount(request));
    }

    /**
     * Explains a ledger account.
     */
    @Override
    public void explainLedgerAccountDecision(ExplainLedgerAccountDecisionRequest request, StreamObserver<ExplainLedgerAccountDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainLedgerAccountDecision(request));
    }

    /**
     * Scores a ledger account.
     */
    @Override
    public void scoreLedgerAccount(ScoreLedgerAccountRequest request, StreamObserver<ScoreLedgerAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreLedgerAccount(request));
    }

    /**
     * Lints a ledger account.
     */
    @Override
    public void lintLedgerAccounts(LintLedgerAccountsRequest request, StreamObserver<LintLedgerAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintLedgerAccounts(request));
    }

    /**
     * Returns a ledger account.
     */
    @Override
    public void getLedgerAccountRules(GetLedgerAccountRulesRequest request, StreamObserver<GetLedgerAccountRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLedgerAccountRules(request));
    }
}
