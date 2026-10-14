package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityRequest;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityResponse;
import com.acme.accounting.v1.ExplainChartOfAccountsDecisionRequest;
import com.acme.accounting.v1.ExplainChartOfAccountsDecisionResponse;
import com.acme.accounting.v1.GetChartOfAccountsRulesRequest;
import com.acme.accounting.v1.GetChartOfAccountsRulesResponse;
import com.acme.accounting.v1.LintChartOfAccountsesRequest;
import com.acme.accounting.v1.LintChartOfAccountsesResponse;
import com.acme.accounting.v1.NormalizeChartOfAccountsRequest;
import com.acme.accounting.v1.NormalizeChartOfAccountsResponse;
import com.acme.accounting.v1.PreviewChartOfAccountsRequest;
import com.acme.accounting.v1.PreviewChartOfAccountsResponse;
import com.acme.accounting.v1.ScoreChartOfAccountsRequest;
import com.acme.accounting.v1.ScoreChartOfAccountsResponse;
import com.acme.accounting.v1.ValidateChartOfAccountsRequest;
import com.acme.accounting.v1.ValidateChartOfAccountsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks chart of accountses against the rules before anything acts on them.
 */
@GrpcService
public class ChartOfAccountsValidationEndpoint extends ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceImplBase {
    protected final ChartOfAccountsValidationApplication application;

    public ChartOfAccountsValidationEndpoint(ChartOfAccountsValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a chart of accounts.
     */
    @Override
    public void validateChartOfAccounts(ValidateChartOfAccountsRequest request, StreamObserver<ValidateChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateChartOfAccounts(request));
    }

    /**
     * Previews a chart of accounts.
     */
    @Override
    public void previewChartOfAccounts(PreviewChartOfAccountsRequest request, StreamObserver<PreviewChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewChartOfAccounts(request));
    }

    /**
     * Checks a chart of accounts.
     */
    @Override
    public void checkChartOfAccountsEligibility(CheckChartOfAccountsEligibilityRequest request, StreamObserver<CheckChartOfAccountsEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkChartOfAccountsEligibility(request));
    }

    /**
     * Normalizes a chart of accounts.
     */
    @Override
    public void normalizeChartOfAccounts(NormalizeChartOfAccountsRequest request, StreamObserver<NormalizeChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeChartOfAccounts(request));
    }

    /**
     * Explains a chart of accounts.
     */
    @Override
    public void explainChartOfAccountsDecision(ExplainChartOfAccountsDecisionRequest request, StreamObserver<ExplainChartOfAccountsDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainChartOfAccountsDecision(request));
    }

    /**
     * Scores a chart of accounts.
     */
    @Override
    public void scoreChartOfAccounts(ScoreChartOfAccountsRequest request, StreamObserver<ScoreChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreChartOfAccounts(request));
    }

    /**
     * Lints a chart of accounts.
     */
    @Override
    public void lintChartOfAccountses(LintChartOfAccountsesRequest request, StreamObserver<LintChartOfAccountsesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintChartOfAccountses(request));
    }

    /**
     * Returns a chart of accounts.
     */
    @Override
    public void getChartOfAccountsRules(GetChartOfAccountsRulesRequest request, StreamObserver<GetChartOfAccountsRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getChartOfAccountsRules(request));
    }
}
