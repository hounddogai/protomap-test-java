package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.CheckEInvoiceEligibilityRequest;
import com.acme.invoicing.v1.CheckEInvoiceEligibilityResponse;
import com.acme.invoicing.v1.EInvoiceValidationServiceGrpc;
import com.acme.invoicing.v1.ExplainEInvoiceDecisionRequest;
import com.acme.invoicing.v1.ExplainEInvoiceDecisionResponse;
import com.acme.invoicing.v1.GetEInvoiceRulesRequest;
import com.acme.invoicing.v1.GetEInvoiceRulesResponse;
import com.acme.invoicing.v1.LintEInvoicesRequest;
import com.acme.invoicing.v1.LintEInvoicesResponse;
import com.acme.invoicing.v1.NormalizeEInvoiceRequest;
import com.acme.invoicing.v1.NormalizeEInvoiceResponse;
import com.acme.invoicing.v1.PreviewEInvoiceRequest;
import com.acme.invoicing.v1.PreviewEInvoiceResponse;
import com.acme.invoicing.v1.ScoreEInvoiceRequest;
import com.acme.invoicing.v1.ScoreEInvoiceResponse;
import com.acme.invoicing.v1.ValidateEInvoiceRequest;
import com.acme.invoicing.v1.ValidateEInvoiceResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks e invoices against the rules before anything acts on them.
 */
@GrpcService
public class EInvoiceValidationEndpoint extends EInvoiceValidationServiceGrpc.EInvoiceValidationServiceImplBase {
    protected final EInvoiceValidationApplication application;

    public EInvoiceValidationEndpoint(EInvoiceValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an e invoice.
     */
    @Override
    public void validateEInvoice(ValidateEInvoiceRequest request, StreamObserver<ValidateEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateEInvoice(request));
    }

    /**
     * Previews an e invoice.
     */
    @Override
    public void previewEInvoice(PreviewEInvoiceRequest request, StreamObserver<PreviewEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewEInvoice(request));
    }

    /**
     * Checks an e invoice.
     */
    @Override
    public void checkEInvoiceEligibility(CheckEInvoiceEligibilityRequest request, StreamObserver<CheckEInvoiceEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkEInvoiceEligibility(request));
    }

    /**
     * Normalizes an e invoice.
     */
    @Override
    public void normalizeEInvoice(NormalizeEInvoiceRequest request, StreamObserver<NormalizeEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeEInvoice(request));
    }

    /**
     * Explains an e invoice.
     */
    @Override
    public void explainEInvoiceDecision(ExplainEInvoiceDecisionRequest request, StreamObserver<ExplainEInvoiceDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainEInvoiceDecision(request));
    }

    /**
     * Scores an e invoice.
     */
    @Override
    public void scoreEInvoice(ScoreEInvoiceRequest request, StreamObserver<ScoreEInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreEInvoice(request));
    }

    /**
     * Lints an e invoice.
     */
    @Override
    public void lintEInvoices(LintEInvoicesRequest request, StreamObserver<LintEInvoicesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintEInvoices(request));
    }

    /**
     * Returns an e invoice.
     */
    @Override
    public void getEInvoiceRules(GetEInvoiceRulesRequest request, StreamObserver<GetEInvoiceRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getEInvoiceRules(request));
    }
}
