package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.CheckExpenseReportEligibilityRequest;
import com.acme.expenses.v1.CheckExpenseReportEligibilityResponse;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.ExplainExpenseReportDecisionRequest;
import com.acme.expenses.v1.ExplainExpenseReportDecisionResponse;
import com.acme.expenses.v1.GetExpenseReportRulesRequest;
import com.acme.expenses.v1.GetExpenseReportRulesResponse;
import com.acme.expenses.v1.LintExpenseReportsRequest;
import com.acme.expenses.v1.LintExpenseReportsResponse;
import com.acme.expenses.v1.PreviewExpenseReportRequest;
import com.acme.expenses.v1.PreviewExpenseReportResponse;
import com.acme.expenses.v1.ScoreExpenseReportRequest;
import com.acme.expenses.v1.ScoreExpenseReportResponse;
import com.acme.expenses.v1.ValidateExpenseReportRequest;
import com.acme.expenses.v1.ValidateExpenseReportResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks expense reports against the rules before anything acts on them.
 */
@GrpcService
public class ExpenseReportValidationEndpoint extends ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceImplBase {
    protected final ExpenseReportValidationApplication application;

    public ExpenseReportValidationEndpoint(ExpenseReportValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an expense report.
     */
    @Override
    public void validateExpenseReport(ValidateExpenseReportRequest request, StreamObserver<ValidateExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateExpenseReport(request));
    }

    /**
     * Previews an expense report.
     */
    @Override
    public void previewExpenseReport(PreviewExpenseReportRequest request, StreamObserver<PreviewExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewExpenseReport(request));
    }

    /**
     * Checks an expense report.
     */
    @Override
    public void checkExpenseReportEligibility(CheckExpenseReportEligibilityRequest request, StreamObserver<CheckExpenseReportEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkExpenseReportEligibility(request));
    }

    /**
     * Explains an expense report.
     */
    @Override
    public void explainExpenseReportDecision(ExplainExpenseReportDecisionRequest request, StreamObserver<ExplainExpenseReportDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainExpenseReportDecision(request));
    }

    /**
     * Scores an expense report.
     */
    @Override
    public void scoreExpenseReport(ScoreExpenseReportRequest request, StreamObserver<ScoreExpenseReportResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreExpenseReport(request));
    }

    /**
     * Lints an expense report.
     */
    @Override
    public void lintExpenseReports(LintExpenseReportsRequest request, StreamObserver<LintExpenseReportsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintExpenseReports(request));
    }

    /**
     * Returns an expense report.
     */
    @Override
    public void getExpenseReportRules(GetExpenseReportRulesRequest request, StreamObserver<GetExpenseReportRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getExpenseReportRules(request));
    }
}
