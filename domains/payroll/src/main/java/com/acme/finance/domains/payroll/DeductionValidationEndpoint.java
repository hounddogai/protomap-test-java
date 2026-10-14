package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.CheckDeductionEligibilityRequest;
import com.acme.payroll.v1.CheckDeductionEligibilityResponse;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.GetDeductionRulesRequest;
import com.acme.payroll.v1.GetDeductionRulesResponse;
import com.acme.payroll.v1.LintDeductionsRequest;
import com.acme.payroll.v1.LintDeductionsResponse;
import com.acme.payroll.v1.NormalizeDeductionRequest;
import com.acme.payroll.v1.NormalizeDeductionResponse;
import com.acme.payroll.v1.PreviewDeductionRequest;
import com.acme.payroll.v1.PreviewDeductionResponse;
import com.acme.payroll.v1.ScoreDeductionRequest;
import com.acme.payroll.v1.ScoreDeductionResponse;
import com.acme.payroll.v1.ValidateDeductionRequest;
import com.acme.payroll.v1.ValidateDeductionResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks deductions against the rules before anything acts on them.
 */
@GrpcService
public class DeductionValidationEndpoint extends DeductionValidationServiceGrpc.DeductionValidationServiceImplBase {
    protected final DeductionValidationApplication application;

    public DeductionValidationEndpoint(DeductionValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a deduction.
     */
    @Override
    public void validateDeduction(ValidateDeductionRequest request, StreamObserver<ValidateDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateDeduction(request));
    }

    /**
     * Previews a deduction.
     */
    @Override
    public void previewDeduction(PreviewDeductionRequest request, StreamObserver<PreviewDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewDeduction(request));
    }

    /**
     * Checks a deduction.
     */
    @Override
    public void checkDeductionEligibility(CheckDeductionEligibilityRequest request, StreamObserver<CheckDeductionEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkDeductionEligibility(request));
    }

    /**
     * Normalizes a deduction.
     */
    @Override
    public void normalizeDeduction(NormalizeDeductionRequest request, StreamObserver<NormalizeDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeDeduction(request));
    }

    /**
     * Scores a deduction.
     */
    @Override
    public void scoreDeduction(ScoreDeductionRequest request, StreamObserver<ScoreDeductionResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreDeduction(request));
    }

    /**
     * Lints a deduction.
     */
    @Override
    public void lintDeductions(LintDeductionsRequest request, StreamObserver<LintDeductionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintDeductions(request));
    }

    /**
     * Returns a deduction.
     */
    @Override
    public void getDeductionRules(GetDeductionRulesRequest request, StreamObserver<GetDeductionRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDeductionRules(request));
    }
}
