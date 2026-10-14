package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.CheckPayRunEligibilityRequest;
import com.acme.payroll.v1.CheckPayRunEligibilityResponse;
import com.acme.payroll.v1.ExplainPayRunDecisionRequest;
import com.acme.payroll.v1.ExplainPayRunDecisionResponse;
import com.acme.payroll.v1.GetPayRunRulesRequest;
import com.acme.payroll.v1.GetPayRunRulesResponse;
import com.acme.payroll.v1.LintPayRunsRequest;
import com.acme.payroll.v1.LintPayRunsResponse;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PreviewPayRunRequest;
import com.acme.payroll.v1.PreviewPayRunResponse;
import com.acme.payroll.v1.ValidatePayRunRequest;
import com.acme.payroll.v1.ValidatePayRunResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks pay runs against the rules before anything acts on them.
 */
@GrpcService
public class PayRunValidationEndpoint extends PayRunValidationServiceGrpc.PayRunValidationServiceImplBase {
    protected final PayRunValidationApplication application;

    public PayRunValidationEndpoint(PayRunValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a pay run.
     */
    @Override
    public void validatePayRun(ValidatePayRunRequest request, StreamObserver<ValidatePayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.validatePayRun(request));
    }

    /**
     * Previews a pay run.
     */
    @Override
    public void previewPayRun(PreviewPayRunRequest request, StreamObserver<PreviewPayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewPayRun(request));
    }

    /**
     * Checks a pay run.
     */
    @Override
    public void checkPayRunEligibility(CheckPayRunEligibilityRequest request, StreamObserver<CheckPayRunEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkPayRunEligibility(request));
    }

    /**
     * Explains a pay run.
     */
    @Override
    public void explainPayRunDecision(ExplainPayRunDecisionRequest request, StreamObserver<ExplainPayRunDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainPayRunDecision(request));
    }

    /**
     * Lints a pay run.
     */
    @Override
    public void lintPayRuns(LintPayRunsRequest request, StreamObserver<LintPayRunsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintPayRuns(request));
    }

    /**
     * Returns a pay run.
     */
    @Override
    public void getPayRunRules(GetPayRunRulesRequest request, StreamObserver<GetPayRunRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPayRunRules(request));
    }
}
