package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.ActivateAttestationRequest;
import com.acme.compliance.v1.ActivateAttestationResponse;
import com.acme.compliance.v1.AttestationLifecycleServiceGrpc;
import com.acme.compliance.v1.ExpireAttestationRequest;
import com.acme.compliance.v1.ExpireAttestationResponse;
import com.acme.compliance.v1.GetAttestationLifecycleRequest;
import com.acme.compliance.v1.GetAttestationLifecycleResponse;
import com.acme.compliance.v1.RenewAttestationRequest;
import com.acme.compliance.v1.RenewAttestationResponse;
import com.acme.compliance.v1.SuspendAttestationRequest;
import com.acme.compliance.v1.SuspendAttestationResponse;
import com.acme.compliance.v1.TerminateAttestationRequest;
import com.acme.compliance.v1.TerminateAttestationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Moves an attestation between the states of its lifecycle.
 */
@GrpcService
public class AttestationLifecycleEndpoint extends AttestationLifecycleServiceGrpc.AttestationLifecycleServiceImplBase {
    protected final AttestationLifecycleApplication application;

    public AttestationLifecycleEndpoint(AttestationLifecycleApplication application) {
        this.application = application;
    }

    /**
     * Activates an attestation.
     */
    @Override
    public void activateAttestation(ActivateAttestationRequest request, StreamObserver<ActivateAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.activateAttestation(request));
    }

    /**
     * Suspends an attestation.
     */
    @Override
    public void suspendAttestation(SuspendAttestationRequest request, StreamObserver<SuspendAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.suspendAttestation(request));
    }

    /**
     * Terminates an attestation.
     */
    @Override
    public void terminateAttestation(TerminateAttestationRequest request, StreamObserver<TerminateAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.terminateAttestation(request));
    }

    /**
     * Renews an attestation.
     */
    @Override
    public void renewAttestation(RenewAttestationRequest request, StreamObserver<RenewAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.renewAttestation(request));
    }

    /**
     * Expires an attestation.
     */
    @Override
    public void expireAttestation(ExpireAttestationRequest request, StreamObserver<ExpireAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.expireAttestation(request));
    }

    /**
     * Returns an attestation.
     */
    @Override
    public void getAttestationLifecycle(GetAttestationLifecycleRequest request, StreamObserver<GetAttestationLifecycleResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAttestationLifecycle(request));
    }
}
