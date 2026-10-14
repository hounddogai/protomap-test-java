package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.CheckTaxWithholdingEligibilityRequest;
import com.acme.payroll.v1.CheckTaxWithholdingEligibilityResponse;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionRequest;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionResponse;
import com.acme.payroll.v1.LintTaxWithholdingsRequest;
import com.acme.payroll.v1.LintTaxWithholdingsResponse;
import com.acme.payroll.v1.NormalizeTaxWithholdingRequest;
import com.acme.payroll.v1.NormalizeTaxWithholdingResponse;
import com.acme.payroll.v1.PreviewTaxWithholdingRequest;
import com.acme.payroll.v1.PreviewTaxWithholdingResponse;
import com.acme.payroll.v1.ScoreTaxWithholdingRequest;
import com.acme.payroll.v1.ScoreTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.payroll.v1.ValidateTaxWithholdingRequest;
import com.acme.payroll.v1.ValidateTaxWithholdingResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks tax withholdings against the rules before anything acts on them.
 */
@GrpcService
public class TaxWithholdingValidationEndpoint extends TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceImplBase {
    protected final TaxWithholdingValidationApplication application;

    public TaxWithholdingValidationEndpoint(TaxWithholdingValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a tax withholding.
     */
    @Override
    public void validateTaxWithholding(ValidateTaxWithholdingRequest request, StreamObserver<ValidateTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateTaxWithholding(request));
    }

    /**
     * Previews a tax withholding.
     */
    @Override
    public void previewTaxWithholding(PreviewTaxWithholdingRequest request, StreamObserver<PreviewTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewTaxWithholding(request));
    }

    /**
     * Checks a tax withholding.
     */
    @Override
    public void checkTaxWithholdingEligibility(CheckTaxWithholdingEligibilityRequest request, StreamObserver<CheckTaxWithholdingEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkTaxWithholdingEligibility(request));
    }

    /**
     * Normalizes a tax withholding.
     */
    @Override
    public void normalizeTaxWithholding(NormalizeTaxWithholdingRequest request, StreamObserver<NormalizeTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeTaxWithholding(request));
    }

    /**
     * Explains a tax withholding.
     */
    @Override
    public void explainTaxWithholdingDecision(ExplainTaxWithholdingDecisionRequest request, StreamObserver<ExplainTaxWithholdingDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainTaxWithholdingDecision(request));
    }

    /**
     * Scores a tax withholding.
     */
    @Override
    public void scoreTaxWithholding(ScoreTaxWithholdingRequest request, StreamObserver<ScoreTaxWithholdingResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreTaxWithholding(request));
    }

    /**
     * Lints a tax withholding.
     */
    @Override
    public void lintTaxWithholdings(LintTaxWithholdingsRequest request, StreamObserver<LintTaxWithholdingsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintTaxWithholdings(request));
    }
}
