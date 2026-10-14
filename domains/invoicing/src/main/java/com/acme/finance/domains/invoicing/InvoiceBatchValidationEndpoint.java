package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchDecisionRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchDecisionResponse;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.LintInvoiceBatchesRequest;
import com.acme.invoicing.v1.LintInvoiceBatchesResponse;
import com.acme.invoicing.v1.NormalizeInvoiceBatchRequest;
import com.acme.invoicing.v1.NormalizeInvoiceBatchResponse;
import com.acme.invoicing.v1.PreviewInvoiceBatchRequest;
import com.acme.invoicing.v1.PreviewInvoiceBatchResponse;
import com.acme.invoicing.v1.ScoreInvoiceBatchRequest;
import com.acme.invoicing.v1.ScoreInvoiceBatchResponse;
import com.acme.invoicing.v1.ValidateInvoiceBatchRequest;
import com.acme.invoicing.v1.ValidateInvoiceBatchResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks invoice batches against the rules before anything acts on them.
 */
@GrpcService
public class InvoiceBatchValidationEndpoint extends InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceImplBase {
    protected final InvoiceBatchValidationApplication application;

    public InvoiceBatchValidationEndpoint(InvoiceBatchValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an invoice batch.
     */
    @Override
    public void validateInvoiceBatch(ValidateInvoiceBatchRequest request, StreamObserver<ValidateInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateInvoiceBatch(request));
    }

    /**
     * Previews an invoice batch.
     */
    @Override
    public void previewInvoiceBatch(PreviewInvoiceBatchRequest request, StreamObserver<PreviewInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewInvoiceBatch(request));
    }

    /**
     * Checks an invoice batch.
     */
    @Override
    public void checkInvoiceBatchEligibility(CheckInvoiceBatchEligibilityRequest request, StreamObserver<CheckInvoiceBatchEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkInvoiceBatchEligibility(request));
    }

    /**
     * Normalizes an invoice batch.
     */
    @Override
    public void normalizeInvoiceBatch(NormalizeInvoiceBatchRequest request, StreamObserver<NormalizeInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeInvoiceBatch(request));
    }

    /**
     * Explains an invoice batch.
     */
    @Override
    public void explainInvoiceBatchDecision(ExplainInvoiceBatchDecisionRequest request, StreamObserver<ExplainInvoiceBatchDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainInvoiceBatchDecision(request));
    }

    /**
     * Scores an invoice batch.
     */
    @Override
    public void scoreInvoiceBatch(ScoreInvoiceBatchRequest request, StreamObserver<ScoreInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreInvoiceBatch(request));
    }

    /**
     * Lints an invoice batch.
     */
    @Override
    public void lintInvoiceBatches(LintInvoiceBatchesRequest request, StreamObserver<LintInvoiceBatchesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintInvoiceBatches(request));
    }
}
