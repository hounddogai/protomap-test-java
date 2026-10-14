package com.acme.finance.domains.banking;

import com.acme.banking.v1.ExplainMandateDecisionRequest;
import com.acme.banking.v1.ExplainMandateDecisionResponse;
import com.acme.banking.v1.GetMandateRulesRequest;
import com.acme.banking.v1.GetMandateRulesResponse;
import com.acme.banking.v1.LintMandatesRequest;
import com.acme.banking.v1.LintMandatesResponse;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.NormalizeMandateRequest;
import com.acme.banking.v1.NormalizeMandateResponse;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks mandates against the rules before anything acts on them.
 */
@GrpcService
public class MandateValidationEndpoint extends MandateValidationServiceGrpc.MandateValidationServiceImplBase {
    protected final MandateValidationApplication application;

    public MandateValidationEndpoint(MandateValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a mandate.
     */
    @Override
    public void validateMandate(ValidateMandateRequest request, StreamObserver<ValidateMandateResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateMandate(request));
    }

    /**
     * Previews a mandate.
     */
    @Override
    public void previewMandate(PreviewMandateRequest request, StreamObserver<PreviewMandateResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewMandate(request));
    }

    /**
     * Normalizes a mandate.
     */
    @Override
    public void normalizeMandate(NormalizeMandateRequest request, StreamObserver<NormalizeMandateResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeMandate(request));
    }

    /**
     * Explains a mandate.
     */
    @Override
    public void explainMandateDecision(ExplainMandateDecisionRequest request, StreamObserver<ExplainMandateDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainMandateDecision(request));
    }

    /**
     * Scores a mandate.
     */
    @Override
    public void scoreMandate(ScoreMandateRequest request, StreamObserver<ScoreMandateResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreMandate(request));
    }

    /**
     * Lints a mandate.
     */
    @Override
    public void lintMandates(LintMandatesRequest request, StreamObserver<LintMandatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintMandates(request));
    }

    /**
     * Returns a mandate.
     */
    @Override
    public void getMandateRules(GetMandateRulesRequest request, StreamObserver<GetMandateRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getMandateRules(request));
    }
}
