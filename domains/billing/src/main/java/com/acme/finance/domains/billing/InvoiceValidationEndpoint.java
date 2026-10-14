package com.acme.finance.domains.billing;

import com.acme.billing.v1.CheckInvoiceEligibilityRequest;
import com.acme.billing.v1.CheckInvoiceEligibilityResponse;
import com.acme.billing.v1.ExplainInvoiceDecisionRequest;
import com.acme.billing.v1.ExplainInvoiceDecisionResponse;
import com.acme.billing.v1.GetInvoiceRulesRequest;
import com.acme.billing.v1.GetInvoiceRulesResponse;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.LintInvoicesRequest;
import com.acme.billing.v1.LintInvoicesResponse;
import com.acme.billing.v1.NormalizeInvoiceRequest;
import com.acme.billing.v1.NormalizeInvoiceResponse;
import com.acme.billing.v1.PreviewInvoiceRequest;
import com.acme.billing.v1.PreviewInvoiceResponse;
import com.acme.billing.v1.ScoreInvoiceRequest;
import com.acme.billing.v1.ScoreInvoiceResponse;
import com.acme.billing.v1.ValidateInvoiceRequest;
import com.acme.billing.v1.ValidateInvoiceResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks invoices against the rules before anything acts on them.
 */
@GrpcService
public class InvoiceValidationEndpoint extends InvoiceValidationServiceGrpc.InvoiceValidationServiceImplBase {
    protected final InvoiceValidationApplication application;

    public InvoiceValidationEndpoint(InvoiceValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an invoice.
     */
    @Override
    public void validateInvoice(ValidateInvoiceRequest request, StreamObserver<ValidateInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateInvoice(request));
    }

    /**
     * Previews an invoice.
     */
    @Override
    public void previewInvoice(PreviewInvoiceRequest request, StreamObserver<PreviewInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewInvoice(request));
    }

    /**
     * Checks an invoice.
     */
    @Override
    public void checkInvoiceEligibility(CheckInvoiceEligibilityRequest request, StreamObserver<CheckInvoiceEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkInvoiceEligibility(request));
    }

    /**
     * Normalizes an invoice.
     */
    @Override
    public void normalizeInvoice(NormalizeInvoiceRequest request, StreamObserver<NormalizeInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeInvoice(request));
    }

    /**
     * Explains an invoice.
     */
    @Override
    public void explainInvoiceDecision(ExplainInvoiceDecisionRequest request, StreamObserver<ExplainInvoiceDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainInvoiceDecision(request));
    }

    /**
     * Scores an invoice.
     */
    @Override
    public void scoreInvoice(ScoreInvoiceRequest request, StreamObserver<ScoreInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreInvoice(request));
    }

    /**
     * Lints an invoice.
     */
    @Override
    public void lintInvoices(LintInvoicesRequest request, StreamObserver<LintInvoicesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintInvoices(request));
    }

    /**
     * Returns an invoice.
     */
    @Override
    public void getInvoiceRules(GetInvoiceRulesRequest request, StreamObserver<GetInvoiceRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getInvoiceRules(request));
    }
}
