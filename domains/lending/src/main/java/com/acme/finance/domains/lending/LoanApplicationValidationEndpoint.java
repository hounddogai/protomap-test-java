package com.acme.finance.domains.lending;

import com.acme.finance.platform.RpcSupport;
import com.acme.lending.v1.CheckLoanApplicationEligibilityRequest;
import com.acme.lending.v1.CheckLoanApplicationEligibilityResponse;
import com.acme.lending.v1.ExplainLoanApplicationDecisionRequest;
import com.acme.lending.v1.ExplainLoanApplicationDecisionResponse;
import com.acme.lending.v1.GetLoanApplicationRulesRequest;
import com.acme.lending.v1.GetLoanApplicationRulesResponse;
import com.acme.lending.v1.LintLoanApplicationsRequest;
import com.acme.lending.v1.LintLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.NormalizeLoanApplicationRequest;
import com.acme.lending.v1.NormalizeLoanApplicationResponse;
import com.acme.lending.v1.PreviewLoanApplicationRequest;
import com.acme.lending.v1.PreviewLoanApplicationResponse;
import com.acme.lending.v1.ValidateLoanApplicationRequest;
import com.acme.lending.v1.ValidateLoanApplicationResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks loan applications against the rules before anything acts on them.
 */
@GrpcService
public class LoanApplicationValidationEndpoint extends LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceImplBase {
    protected final LoanApplicationValidationApplication application;

    public LoanApplicationValidationEndpoint(LoanApplicationValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a loan application.
     */
    @Override
    public void validateLoanApplication(ValidateLoanApplicationRequest request, StreamObserver<ValidateLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateLoanApplication(request));
    }

    /**
     * Previews a loan application.
     */
    @Override
    public void previewLoanApplication(PreviewLoanApplicationRequest request, StreamObserver<PreviewLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewLoanApplication(request));
    }

    /**
     * Checks a loan application.
     */
    @Override
    public void checkLoanApplicationEligibility(CheckLoanApplicationEligibilityRequest request, StreamObserver<CheckLoanApplicationEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkLoanApplicationEligibility(request));
    }

    /**
     * Normalizes a loan application.
     */
    @Override
    public void normalizeLoanApplication(NormalizeLoanApplicationRequest request, StreamObserver<NormalizeLoanApplicationResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeLoanApplication(request));
    }

    /**
     * Explains a loan application.
     */
    @Override
    public void explainLoanApplicationDecision(ExplainLoanApplicationDecisionRequest request, StreamObserver<ExplainLoanApplicationDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainLoanApplicationDecision(request));
    }

    /**
     * Lints a loan application.
     */
    @Override
    public void lintLoanApplications(LintLoanApplicationsRequest request, StreamObserver<LintLoanApplicationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintLoanApplications(request));
    }

    /**
     * Returns a loan application.
     */
    @Override
    public void getLoanApplicationRules(GetLoanApplicationRulesRequest request, StreamObserver<GetLoanApplicationRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getLoanApplicationRules(request));
    }
}
