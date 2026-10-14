package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.CheckComplianceCaseEligibilityRequest;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityResponse;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.ExplainComplianceCaseDecisionRequest;
import com.acme.compliance.v1.ExplainComplianceCaseDecisionResponse;
import com.acme.compliance.v1.GetComplianceCaseRulesRequest;
import com.acme.compliance.v1.GetComplianceCaseRulesResponse;
import com.acme.compliance.v1.LintComplianceCasesRequest;
import com.acme.compliance.v1.LintComplianceCasesResponse;
import com.acme.compliance.v1.NormalizeComplianceCaseRequest;
import com.acme.compliance.v1.NormalizeComplianceCaseResponse;
import com.acme.compliance.v1.PreviewComplianceCaseRequest;
import com.acme.compliance.v1.PreviewComplianceCaseResponse;
import com.acme.compliance.v1.ValidateComplianceCaseRequest;
import com.acme.compliance.v1.ValidateComplianceCaseResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks compliance cases against the rules before anything acts on them.
 */
@GrpcService
public class ComplianceCaseValidationEndpoint extends ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceImplBase {
    protected final ComplianceCaseValidationApplication application;

    public ComplianceCaseValidationEndpoint(ComplianceCaseValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a compliance case.
     */
    @Override
    public void validateComplianceCase(ValidateComplianceCaseRequest request, StreamObserver<ValidateComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateComplianceCase(request));
    }

    /**
     * Previews a compliance case.
     */
    @Override
    public void previewComplianceCase(PreviewComplianceCaseRequest request, StreamObserver<PreviewComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewComplianceCase(request));
    }

    /**
     * Checks a compliance case.
     */
    @Override
    public void checkComplianceCaseEligibility(CheckComplianceCaseEligibilityRequest request, StreamObserver<CheckComplianceCaseEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkComplianceCaseEligibility(request));
    }

    /**
     * Normalizes a compliance case.
     */
    @Override
    public void normalizeComplianceCase(NormalizeComplianceCaseRequest request, StreamObserver<NormalizeComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeComplianceCase(request));
    }

    /**
     * Explains a compliance case.
     */
    @Override
    public void explainComplianceCaseDecision(ExplainComplianceCaseDecisionRequest request, StreamObserver<ExplainComplianceCaseDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainComplianceCaseDecision(request));
    }

    /**
     * Lints a compliance case.
     */
    @Override
    public void lintComplianceCases(LintComplianceCasesRequest request, StreamObserver<LintComplianceCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintComplianceCases(request));
    }

    /**
     * Returns a compliance case.
     */
    @Override
    public void getComplianceCaseRules(GetComplianceCaseRulesRequest request, StreamObserver<GetComplianceCaseRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getComplianceCaseRules(request));
    }
}
