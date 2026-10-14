package com.acme.finance.domains.suppliers;

import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.ApproveSupplierRiskRequest;
import com.acme.suppliers.v1.ApproveSupplierRiskResponse;
import com.acme.suppliers.v1.CommentSupplierRiskApprovalRequest;
import com.acme.suppliers.v1.CommentSupplierRiskApprovalResponse;
import com.acme.suppliers.v1.EscalateSupplierRiskRequest;
import com.acme.suppliers.v1.EscalateSupplierRiskResponse;
import com.acme.suppliers.v1.GetSupplierRiskApprovalStatusRequest;
import com.acme.suppliers.v1.GetSupplierRiskApprovalStatusResponse;
import com.acme.suppliers.v1.ListPendingSupplierRisksRequest;
import com.acme.suppliers.v1.ListPendingSupplierRisksResponse;
import com.acme.suppliers.v1.RecallSupplierRiskRequest;
import com.acme.suppliers.v1.RecallSupplierRiskResponse;
import com.acme.suppliers.v1.RejectSupplierRiskRequest;
import com.acme.suppliers.v1.RejectSupplierRiskResponse;
import com.acme.suppliers.v1.SubmitSupplierRiskRequest;
import com.acme.suppliers.v1.SubmitSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierRiskApprovalServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes supplier risks through approval, with escalation and delegation.
 */
@GrpcService
public class SupplierRiskApprovalEndpoint extends SupplierRiskApprovalServiceGrpc.SupplierRiskApprovalServiceImplBase {
    protected final SupplierRiskApprovalApplication application;

    public SupplierRiskApprovalEndpoint(SupplierRiskApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a supplier risk.
     */
    @Override
    public void submitSupplierRisk(SubmitSupplierRiskRequest request, StreamObserver<SubmitSupplierRiskResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitSupplierRisk(request));
    }

    /**
     * Approves a supplier risk.
     */
    @Override
    public void approveSupplierRisk(ApproveSupplierRiskRequest request, StreamObserver<ApproveSupplierRiskResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveSupplierRisk(request));
    }

    /**
     * Rejects a supplier risk.
     */
    @Override
    public void rejectSupplierRisk(RejectSupplierRiskRequest request, StreamObserver<RejectSupplierRiskResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectSupplierRisk(request));
    }

    /**
     * Escalates a supplier risk.
     */
    @Override
    public void escalateSupplierRisk(EscalateSupplierRiskRequest request, StreamObserver<EscalateSupplierRiskResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateSupplierRisk(request));
    }

    /**
     * Lists supplier risks a page at a time.
     */
    @Override
    public void listPendingSupplierRisks(ListPendingSupplierRisksRequest request, StreamObserver<ListPendingSupplierRisksResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingSupplierRisks(request));
    }

    /**
     * Recalls a supplier risk.
     */
    @Override
    public void recallSupplierRisk(RecallSupplierRiskRequest request, StreamObserver<RecallSupplierRiskResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallSupplierRisk(request));
    }

    /**
     * Returns a supplier risk.
     */
    @Override
    public void getSupplierRiskApprovalStatus(GetSupplierRiskApprovalStatusRequest request, StreamObserver<GetSupplierRiskApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getSupplierRiskApprovalStatus(request));
    }

    /**
     * Comments a supplier risk.
     */
    @Override
    public void commentSupplierRiskApproval(CommentSupplierRiskApprovalRequest request, StreamObserver<CommentSupplierRiskApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentSupplierRiskApproval(request));
    }
}
