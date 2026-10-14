package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.ApproveAttestationRequest;
import com.acme.compliance.v1.ApproveAttestationResponse;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.CommentAttestationApprovalRequest;
import com.acme.compliance.v1.CommentAttestationApprovalResponse;
import com.acme.compliance.v1.DelegateAttestationApprovalRequest;
import com.acme.compliance.v1.DelegateAttestationApprovalResponse;
import com.acme.compliance.v1.EscalateAttestationRequest;
import com.acme.compliance.v1.EscalateAttestationResponse;
import com.acme.compliance.v1.GetAttestationApprovalStatusRequest;
import com.acme.compliance.v1.GetAttestationApprovalStatusResponse;
import com.acme.compliance.v1.RejectAttestationRequest;
import com.acme.compliance.v1.RejectAttestationResponse;
import com.acme.compliance.v1.SubmitAttestationRequest;
import com.acme.compliance.v1.SubmitAttestationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes attestations through approval, with escalation and delegation.
 */
@GrpcService
public class AttestationApprovalEndpoint extends AttestationApprovalServiceGrpc.AttestationApprovalServiceImplBase {
    protected final AttestationApprovalApplication application;

    public AttestationApprovalEndpoint(AttestationApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits an attestation.
     */
    @Override
    public void submitAttestation(SubmitAttestationRequest request, StreamObserver<SubmitAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitAttestation(request));
    }

    /**
     * Approves an attestation.
     */
    @Override
    public void approveAttestation(ApproveAttestationRequest request, StreamObserver<ApproveAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveAttestation(request));
    }

    /**
     * Rejects an attestation.
     */
    @Override
    public void rejectAttestation(RejectAttestationRequest request, StreamObserver<RejectAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectAttestation(request));
    }

    /**
     * Escalates an attestation.
     */
    @Override
    public void escalateAttestation(EscalateAttestationRequest request, StreamObserver<EscalateAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateAttestation(request));
    }

    /**
     * Delegates an attestation.
     */
    @Override
    public void delegateAttestationApproval(DelegateAttestationApprovalRequest request, StreamObserver<DelegateAttestationApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateAttestationApproval(request));
    }

    /**
     * Returns an attestation.
     */
    @Override
    public void getAttestationApprovalStatus(GetAttestationApprovalStatusRequest request, StreamObserver<GetAttestationApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAttestationApprovalStatus(request));
    }

    /**
     * Comments an attestation.
     */
    @Override
    public void commentAttestationApproval(CommentAttestationApprovalRequest request, StreamObserver<CommentAttestationApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentAttestationApproval(request));
    }
}
