package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.AttestationValidationServiceGrpc;
import com.acme.compliance.v1.ExplainAttestationDecisionRequest;
import com.acme.compliance.v1.ExplainAttestationDecisionResponse;
import com.acme.compliance.v1.GetAttestationRulesRequest;
import com.acme.compliance.v1.GetAttestationRulesResponse;
import com.acme.compliance.v1.LintAttestationsRequest;
import com.acme.compliance.v1.LintAttestationsResponse;
import com.acme.compliance.v1.NormalizeAttestationRequest;
import com.acme.compliance.v1.NormalizeAttestationResponse;
import com.acme.compliance.v1.PreviewAttestationRequest;
import com.acme.compliance.v1.PreviewAttestationResponse;
import com.acme.compliance.v1.ValidateAttestationRequest;
import com.acme.compliance.v1.ValidateAttestationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Checks attestations against the rules before anything acts on them.
 */
public abstract class AbstractAttestationValidationEndpoint extends AttestationValidationServiceGrpc.AttestationValidationServiceImplBase {
    protected final AttestationValidationApplication application;

    protected AbstractAttestationValidationEndpoint(AttestationValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an attestation.
     */
    @Override
    public void validateAttestation(ValidateAttestationRequest request, StreamObserver<ValidateAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateAttestation(request));
    }

    /**
     * Previews an attestation.
     */
    @Override
    public void previewAttestation(PreviewAttestationRequest request, StreamObserver<PreviewAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewAttestation(request));
    }

    /**
     * Normalizes an attestation.
     */
    @Override
    public void normalizeAttestation(NormalizeAttestationRequest request, StreamObserver<NormalizeAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeAttestation(request));
    }

    /**
     * Explains an attestation.
     */
    @Override
    public void explainAttestationDecision(ExplainAttestationDecisionRequest request, StreamObserver<ExplainAttestationDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainAttestationDecision(request));
    }

    /**
     * Lints an attestation.
     */
    @Override
    public void lintAttestations(LintAttestationsRequest request, StreamObserver<LintAttestationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintAttestations(request));
    }

    /**
     * Returns an attestation.
     */
    @Override
    public void getAttestationRules(GetAttestationRulesRequest request, StreamObserver<GetAttestationRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAttestationRules(request));
    }
}
