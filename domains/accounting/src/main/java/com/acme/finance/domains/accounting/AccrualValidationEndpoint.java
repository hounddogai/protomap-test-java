package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.CheckAccrualEligibilityRequest;
import com.acme.accounting.v1.CheckAccrualEligibilityResponse;
import com.acme.accounting.v1.ExplainAccrualDecisionRequest;
import com.acme.accounting.v1.ExplainAccrualDecisionResponse;
import com.acme.accounting.v1.GetAccrualRulesRequest;
import com.acme.accounting.v1.GetAccrualRulesResponse;
import com.acme.accounting.v1.LintAccrualsRequest;
import com.acme.accounting.v1.LintAccrualsResponse;
import com.acme.accounting.v1.NormalizeAccrualRequest;
import com.acme.accounting.v1.NormalizeAccrualResponse;
import com.acme.accounting.v1.PreviewAccrualRequest;
import com.acme.accounting.v1.PreviewAccrualResponse;
import com.acme.accounting.v1.ScoreAccrualRequest;
import com.acme.accounting.v1.ScoreAccrualResponse;
import com.acme.accounting.v1.ValidateAccrualRequest;
import com.acme.accounting.v1.ValidateAccrualResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks accruals against the rules before anything acts on them.
 */
@GrpcService
public class AccrualValidationEndpoint extends AccrualValidationServiceGrpc.AccrualValidationServiceImplBase {
    protected final AccrualValidationApplication application;

    public AccrualValidationEndpoint(AccrualValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an accrual.
     */
    @Override
    public void validateAccrual(ValidateAccrualRequest request, StreamObserver<ValidateAccrualResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateAccrual(request));
    }

    /**
     * Previews an accrual.
     */
    @Override
    public void previewAccrual(PreviewAccrualRequest request, StreamObserver<PreviewAccrualResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewAccrual(request));
    }

    /**
     * Checks an accrual.
     */
    @Override
    public void checkAccrualEligibility(CheckAccrualEligibilityRequest request, StreamObserver<CheckAccrualEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkAccrualEligibility(request));
    }

    /**
     * Normalizes an accrual.
     */
    @Override
    public void normalizeAccrual(NormalizeAccrualRequest request, StreamObserver<NormalizeAccrualResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeAccrual(request));
    }

    /**
     * Explains an accrual.
     */
    @Override
    public void explainAccrualDecision(ExplainAccrualDecisionRequest request, StreamObserver<ExplainAccrualDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainAccrualDecision(request));
    }

    /**
     * Scores an accrual.
     */
    @Override
    public void scoreAccrual(ScoreAccrualRequest request, StreamObserver<ScoreAccrualResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreAccrual(request));
    }

    /**
     * Lints an accrual.
     */
    @Override
    public void lintAccruals(LintAccrualsRequest request, StreamObserver<LintAccrualsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintAccruals(request));
    }

    /**
     * Returns an accrual.
     */
    @Override
    public void getAccrualRules(GetAccrualRulesRequest request, StreamObserver<GetAccrualRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAccrualRules(request));
    }
}
