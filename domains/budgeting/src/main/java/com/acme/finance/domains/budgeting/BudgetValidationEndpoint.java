package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.CheckBudgetEligibilityRequest;
import com.acme.budgeting.v1.CheckBudgetEligibilityResponse;
import com.acme.budgeting.v1.GetBudgetRulesRequest;
import com.acme.budgeting.v1.GetBudgetRulesResponse;
import com.acme.budgeting.v1.LintBudgetsRequest;
import com.acme.budgeting.v1.LintBudgetsResponse;
import com.acme.budgeting.v1.NormalizeBudgetRequest;
import com.acme.budgeting.v1.NormalizeBudgetResponse;
import com.acme.budgeting.v1.PreviewBudgetRequest;
import com.acme.budgeting.v1.PreviewBudgetResponse;
import com.acme.budgeting.v1.ScoreBudgetRequest;
import com.acme.budgeting.v1.ScoreBudgetResponse;
import com.acme.budgeting.v1.ValidateBudgetRequest;
import com.acme.budgeting.v1.ValidateBudgetResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks budgets against the rules before anything acts on them.
 */
@GrpcService
public class BudgetValidationEndpoint extends BudgetValidationServiceGrpc.BudgetValidationServiceImplBase {
    protected final BudgetValidationApplication application;

    public BudgetValidationEndpoint(BudgetValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a budget.
     */
    @Override
    public void validateBudget(ValidateBudgetRequest request, StreamObserver<ValidateBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateBudget(request));
    }

    /**
     * Previews a budget.
     */
    @Override
    public void previewBudget(PreviewBudgetRequest request, StreamObserver<PreviewBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewBudget(request));
    }

    /**
     * Checks a budget.
     */
    @Override
    public void checkBudgetEligibility(CheckBudgetEligibilityRequest request, StreamObserver<CheckBudgetEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkBudgetEligibility(request));
    }

    /**
     * Normalizes a budget.
     */
    @Override
    public void normalizeBudget(NormalizeBudgetRequest request, StreamObserver<NormalizeBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeBudget(request));
    }

    /**
     * Scores a budget.
     */
    @Override
    public void scoreBudget(ScoreBudgetRequest request, StreamObserver<ScoreBudgetResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreBudget(request));
    }

    /**
     * Lints a budget.
     */
    @Override
    public void lintBudgets(LintBudgetsRequest request, StreamObserver<LintBudgetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintBudgets(request));
    }

    /**
     * Returns a budget.
     */
    @Override
    public void getBudgetRules(GetBudgetRulesRequest request, StreamObserver<GetBudgetRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getBudgetRules(request));
    }
}
