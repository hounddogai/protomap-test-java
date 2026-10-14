package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.CheckLoanEligibilityRequest;
import com.acme.lending.v1.CheckLoanEligibilityResponse;
import com.acme.lending.v1.ExplainLoanDecisionRequest;
import com.acme.lending.v1.ExplainLoanDecisionResponse;
import com.acme.lending.v1.GetLoanRulesRequest;
import com.acme.lending.v1.GetLoanRulesResponse;
import com.acme.lending.v1.LintLoansRequest;
import com.acme.lending.v1.LintLoansResponse;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.NormalizeLoanRequest;
import com.acme.lending.v1.NormalizeLoanResponse;
import com.acme.lending.v1.PreviewLoanRequest;
import com.acme.lending.v1.PreviewLoanResponse;
import com.acme.lending.v1.ScoreLoanRequest;
import com.acme.lending.v1.ScoreLoanResponse;
import com.acme.lending.v1.ValidateLoanRequest;
import com.acme.lending.v1.ValidateLoanResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks loans against the rules before anything acts on them.
 */
@GrpcService
public class LoanValidationEndpoint extends LoanValidationServiceGrpc.LoanValidationServiceImplBase {
    protected final LoanValidationApplication application;

    public LoanValidationEndpoint(LoanValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a loan.
     */
    @Override
    public void validateLoan(ValidateLoanRequest request, StreamObserver<ValidateLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateLoan(request));
    }

    /**
     * Previews a loan.
     */
    @Override
    public void previewLoan(PreviewLoanRequest request, StreamObserver<PreviewLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewLoan(request));
    }

    /**
     * Checks a loan.
     */
    @Override
    public void checkLoanEligibility(CheckLoanEligibilityRequest request, StreamObserver<CheckLoanEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkLoanEligibility(request));
    }

    /**
     * Normalizes a loan.
     */
    @Override
    public void normalizeLoan(NormalizeLoanRequest request, StreamObserver<NormalizeLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeLoan(request));
    }

    /**
     * Explains a loan.
     */
    @Override
    public void explainLoanDecision(ExplainLoanDecisionRequest request, StreamObserver<ExplainLoanDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainLoanDecision(request));
    }

    /**
     * Scores a loan.
     */
    @Override
    public void scoreLoan(ScoreLoanRequest request, StreamObserver<ScoreLoanResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreLoan(request));
    }

    /**
     * Lints a loan.
     */
    @Override
    public void lintLoans(LintLoansRequest request, StreamObserver<LintLoansResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintLoans(request));
    }

    /**
     * Returns a loan.
     */
    @Override
    public void getLoanRules(GetLoanRulesRequest request, StreamObserver<GetLoanRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLoanRules(request));
    }
}
