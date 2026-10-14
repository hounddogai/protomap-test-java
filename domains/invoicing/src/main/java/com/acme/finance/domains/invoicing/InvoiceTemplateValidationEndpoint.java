package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityResponse;
import com.acme.invoicing.v1.ExplainInvoiceTemplateDecisionRequest;
import com.acme.invoicing.v1.ExplainInvoiceTemplateDecisionResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateRulesRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateRulesResponse;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.LintInvoiceTemplatesRequest;
import com.acme.invoicing.v1.LintInvoiceTemplatesResponse;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateRequest;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateResponse;
import com.acme.invoicing.v1.PreviewInvoiceTemplateRequest;
import com.acme.invoicing.v1.PreviewInvoiceTemplateResponse;
import com.acme.invoicing.v1.ScoreInvoiceTemplateRequest;
import com.acme.invoicing.v1.ScoreInvoiceTemplateResponse;
import com.acme.invoicing.v1.ValidateInvoiceTemplateRequest;
import com.acme.invoicing.v1.ValidateInvoiceTemplateResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks invoice templates against the rules before anything acts on them.
 */
@GrpcService
public class InvoiceTemplateValidationEndpoint extends InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceImplBase {
    protected final InvoiceTemplateValidationApplication application;

    public InvoiceTemplateValidationEndpoint(InvoiceTemplateValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an invoice template.
     */
    @Override
    public void validateInvoiceTemplate(ValidateInvoiceTemplateRequest request, StreamObserver<ValidateInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateInvoiceTemplate(request));
    }

    /**
     * Previews an invoice template.
     */
    @Override
    public void previewInvoiceTemplate(PreviewInvoiceTemplateRequest request, StreamObserver<PreviewInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewInvoiceTemplate(request));
    }

    /**
     * Checks an invoice template.
     */
    @Override
    public void checkInvoiceTemplateEligibility(CheckInvoiceTemplateEligibilityRequest request, StreamObserver<CheckInvoiceTemplateEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkInvoiceTemplateEligibility(request));
    }

    /**
     * Normalizes an invoice template.
     */
    @Override
    public void normalizeInvoiceTemplate(NormalizeInvoiceTemplateRequest request, StreamObserver<NormalizeInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeInvoiceTemplate(request));
    }

    /**
     * Explains an invoice template.
     */
    @Override
    public void explainInvoiceTemplateDecision(ExplainInvoiceTemplateDecisionRequest request, StreamObserver<ExplainInvoiceTemplateDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainInvoiceTemplateDecision(request));
    }

    /**
     * Scores an invoice template.
     */
    @Override
    public void scoreInvoiceTemplate(ScoreInvoiceTemplateRequest request, StreamObserver<ScoreInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreInvoiceTemplate(request));
    }

    /**
     * Lints an invoice template.
     */
    @Override
    public void lintInvoiceTemplates(LintInvoiceTemplatesRequest request, StreamObserver<LintInvoiceTemplatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintInvoiceTemplates(request));
    }

    /**
     * Returns an invoice template.
     */
    @Override
    public void getInvoiceTemplateRules(GetInvoiceTemplateRulesRequest request, StreamObserver<GetInvoiceTemplateRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getInvoiceTemplateRules(request));
    }
}
