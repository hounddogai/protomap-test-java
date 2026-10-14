package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.CheckBudgetLineEligibilityRequest;
import com.acme.budgeting.v1.CheckBudgetLineEligibilityResponse;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionRequest;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionResponse;
import com.acme.budgeting.v1.NormalizeBudgetLineRequest;
import com.acme.budgeting.v1.NormalizeBudgetLineResponse;
import com.acme.budgeting.v1.PreviewBudgetLineRequest;
import com.acme.budgeting.v1.PreviewBudgetLineResponse;
import com.acme.budgeting.v1.ScoreBudgetLineRequest;
import com.acme.budgeting.v1.ScoreBudgetLineResponse;
import com.acme.budgeting.v1.ValidateBudgetLineRequest;
import com.acme.budgeting.v1.ValidateBudgetLineResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks budget lines against the rules before anything acts on them.
 */
@GrpcService
public class BudgetLineValidationEndpoint extends BudgetLineValidationServiceGrpc.BudgetLineValidationServiceImplBase {
    protected final BudgetLineValidationApplication application;

    public BudgetLineValidationEndpoint(BudgetLineValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a budget line.
     */
    @Override
    public void validateBudgetLine(ValidateBudgetLineRequest request, StreamObserver<ValidateBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateBudgetLine(request));
    }

    /**
     * Previews a budget line.
     */
    @Override
    public void previewBudgetLine(PreviewBudgetLineRequest request, StreamObserver<PreviewBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewBudgetLine(request));
    }

    /**
     * Checks a budget line.
     */
    @Override
    public void checkBudgetLineEligibility(CheckBudgetLineEligibilityRequest request, StreamObserver<CheckBudgetLineEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkBudgetLineEligibility(request));
    }

    /**
     * Normalizes a budget line.
     */
    @Override
    public void normalizeBudgetLine(NormalizeBudgetLineRequest request, StreamObserver<NormalizeBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeBudgetLine(request));
    }

    /**
     * Explains a budget line.
     */
    @Override
    public void explainBudgetLineDecision(ExplainBudgetLineDecisionRequest request, StreamObserver<ExplainBudgetLineDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainBudgetLineDecision(request));
    }

    /**
     * Scores a budget line.
     */
    @Override
    public void scoreBudgetLine(ScoreBudgetLineRequest request, StreamObserver<ScoreBudgetLineResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreBudgetLine(request));
    }
}
