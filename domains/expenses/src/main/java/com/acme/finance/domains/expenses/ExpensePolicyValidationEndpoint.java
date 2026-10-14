package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.CheckExpensePolicyEligibilityRequest;
import com.acme.expenses.v1.CheckExpensePolicyEligibilityResponse;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionRequest;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionResponse;
import com.acme.expenses.v1.GetExpensePolicyRulesRequest;
import com.acme.expenses.v1.GetExpensePolicyRulesResponse;
import com.acme.expenses.v1.LintExpensePoliciesRequest;
import com.acme.expenses.v1.LintExpensePoliciesResponse;
import com.acme.expenses.v1.NormalizeExpensePolicyRequest;
import com.acme.expenses.v1.NormalizeExpensePolicyResponse;
import com.acme.expenses.v1.PreviewExpensePolicyRequest;
import com.acme.expenses.v1.PreviewExpensePolicyResponse;
import com.acme.expenses.v1.ScoreExpensePolicyRequest;
import com.acme.expenses.v1.ScoreExpensePolicyResponse;
import com.acme.expenses.v1.ValidateExpensePolicyRequest;
import com.acme.expenses.v1.ValidateExpensePolicyResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks expense policies against the rules before anything acts on them.
 */
@GrpcService
public class ExpensePolicyValidationEndpoint extends ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceImplBase {
    protected final ExpensePolicyValidationApplication application;

    public ExpensePolicyValidationEndpoint(ExpensePolicyValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an expense policy.
     */
    @Override
    public void validateExpensePolicy(ValidateExpensePolicyRequest request, StreamObserver<ValidateExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateExpensePolicy(request));
    }

    /**
     * Previews an expense policy.
     */
    @Override
    public void previewExpensePolicy(PreviewExpensePolicyRequest request, StreamObserver<PreviewExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewExpensePolicy(request));
    }

    /**
     * Checks an expense policy.
     */
    @Override
    public void checkExpensePolicyEligibility(CheckExpensePolicyEligibilityRequest request, StreamObserver<CheckExpensePolicyEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkExpensePolicyEligibility(request));
    }

    /**
     * Normalizes an expense policy.
     */
    @Override
    public void normalizeExpensePolicy(NormalizeExpensePolicyRequest request, StreamObserver<NormalizeExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeExpensePolicy(request));
    }

    /**
     * Explains an expense policy.
     */
    @Override
    public void explainExpensePolicyDecision(ExplainExpensePolicyDecisionRequest request, StreamObserver<ExplainExpensePolicyDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainExpensePolicyDecision(request));
    }

    /**
     * Scores an expense policy.
     */
    @Override
    public void scoreExpensePolicy(ScoreExpensePolicyRequest request, StreamObserver<ScoreExpensePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreExpensePolicy(request));
    }

    /**
     * Lints an expense policy.
     */
    @Override
    public void lintExpensePolicies(LintExpensePoliciesRequest request, StreamObserver<LintExpensePoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintExpensePolicies(request));
    }

    /**
     * Returns an expense policy.
     */
    @Override
    public void getExpensePolicyRules(GetExpensePolicyRulesRequest request, StreamObserver<GetExpensePolicyRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getExpensePolicyRules(request));
    }
}
