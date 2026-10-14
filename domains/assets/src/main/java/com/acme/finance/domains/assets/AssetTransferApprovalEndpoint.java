package com.acme.finance.domains.assets;

import com.acme.assets.v1.ApproveAssetTransferRequest;
import com.acme.assets.v1.ApproveAssetTransferResponse;
import com.acme.assets.v1.AssetTransferApprovalServiceGrpc;
import com.acme.assets.v1.DelegateAssetTransferApprovalRequest;
import com.acme.assets.v1.DelegateAssetTransferApprovalResponse;
import com.acme.assets.v1.EscalateAssetTransferRequest;
import com.acme.assets.v1.EscalateAssetTransferResponse;
import com.acme.assets.v1.GetAssetTransferApprovalStatusRequest;
import com.acme.assets.v1.GetAssetTransferApprovalStatusResponse;
import com.acme.assets.v1.ListPendingAssetTransfersRequest;
import com.acme.assets.v1.ListPendingAssetTransfersResponse;
import com.acme.assets.v1.RejectAssetTransferRequest;
import com.acme.assets.v1.RejectAssetTransferResponse;
import com.acme.assets.v1.SubmitAssetTransferRequest;
import com.acme.assets.v1.SubmitAssetTransferResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes asset transfers through approval, with escalation and delegation.
 */
@GrpcService
public class AssetTransferApprovalEndpoint extends AssetTransferApprovalServiceGrpc.AssetTransferApprovalServiceImplBase {
    protected final AssetTransferApprovalApplication application;

    public AssetTransferApprovalEndpoint(AssetTransferApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits an asset transfer.
     */
    @Override
    public void submitAssetTransfer(SubmitAssetTransferRequest request, StreamObserver<SubmitAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitAssetTransfer(request));
    }

    /**
     * Approves an asset transfer.
     */
    @Override
    public void approveAssetTransfer(ApproveAssetTransferRequest request, StreamObserver<ApproveAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveAssetTransfer(request));
    }

    /**
     * Rejects an asset transfer.
     */
    @Override
    public void rejectAssetTransfer(RejectAssetTransferRequest request, StreamObserver<RejectAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectAssetTransfer(request));
    }

    /**
     * Escalates an asset transfer.
     */
    @Override
    public void escalateAssetTransfer(EscalateAssetTransferRequest request, StreamObserver<EscalateAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateAssetTransfer(request));
    }

    /**
     * Lists asset transfers a page at a time.
     */
    @Override
    public void listPendingAssetTransfers(ListPendingAssetTransfersRequest request, StreamObserver<ListPendingAssetTransfersResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingAssetTransfers(request));
    }

    /**
     * Delegates an asset transfer.
     */
    @Override
    public void delegateAssetTransferApproval(DelegateAssetTransferApprovalRequest request, StreamObserver<DelegateAssetTransferApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateAssetTransferApproval(request));
    }

    /**
     * Returns an asset transfer.
     */
    @Override
    public void getAssetTransferApprovalStatus(GetAssetTransferApprovalStatusRequest request, StreamObserver<GetAssetTransferApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAssetTransferApprovalStatus(request));
    }
}
