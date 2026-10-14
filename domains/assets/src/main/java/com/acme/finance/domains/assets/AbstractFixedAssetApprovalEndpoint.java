package com.acme.finance.domains.assets;

import com.acme.assets.v1.ApproveFixedAssetRequest;
import com.acme.assets.v1.ApproveFixedAssetResponse;
import com.acme.assets.v1.CommentFixedAssetApprovalRequest;
import com.acme.assets.v1.CommentFixedAssetApprovalResponse;
import com.acme.assets.v1.DelegateFixedAssetApprovalRequest;
import com.acme.assets.v1.DelegateFixedAssetApprovalResponse;
import com.acme.assets.v1.EscalateFixedAssetRequest;
import com.acme.assets.v1.EscalateFixedAssetResponse;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.GetFixedAssetApprovalStatusRequest;
import com.acme.assets.v1.GetFixedAssetApprovalStatusResponse;
import com.acme.assets.v1.ListPendingFixedAssetsRequest;
import com.acme.assets.v1.ListPendingFixedAssetsResponse;
import com.acme.assets.v1.RecallFixedAssetRequest;
import com.acme.assets.v1.RecallFixedAssetResponse;
import com.acme.assets.v1.RejectFixedAssetRequest;
import com.acme.assets.v1.RejectFixedAssetResponse;
import com.acme.assets.v1.SubmitFixedAssetRequest;
import com.acme.assets.v1.SubmitFixedAssetResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Routes fixed assets through approval, with escalation and delegation.
 */
public abstract class AbstractFixedAssetApprovalEndpoint extends FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceImplBase {
    protected final FixedAssetApprovalApplication application;

    protected AbstractFixedAssetApprovalEndpoint(FixedAssetApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a fixed asset.
     */
    @Override
    public void submitFixedAsset(SubmitFixedAssetRequest request, StreamObserver<SubmitFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitFixedAsset(request));
    }

    /**
     * Approves a fixed asset.
     */
    @Override
    public void approveFixedAsset(ApproveFixedAssetRequest request, StreamObserver<ApproveFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveFixedAsset(request));
    }

    /**
     * Rejects a fixed asset.
     */
    @Override
    public void rejectFixedAsset(RejectFixedAssetRequest request, StreamObserver<RejectFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectFixedAsset(request));
    }

    /**
     * Escalates a fixed asset.
     */
    @Override
    public void escalateFixedAsset(EscalateFixedAssetRequest request, StreamObserver<EscalateFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateFixedAsset(request));
    }

    /**
     * Lists fixed assets a page at a time.
     */
    @Override
    public void listPendingFixedAssets(ListPendingFixedAssetsRequest request, StreamObserver<ListPendingFixedAssetsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPendingFixedAssets(request));
    }

    /**
     * Delegates a fixed asset.
     */
    @Override
    public void delegateFixedAssetApproval(DelegateFixedAssetApprovalRequest request, StreamObserver<DelegateFixedAssetApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateFixedAssetApproval(request));
    }

    /**
     * Recalls a fixed asset.
     */
    @Override
    public void recallFixedAsset(RecallFixedAssetRequest request, StreamObserver<RecallFixedAssetResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallFixedAsset(request));
    }

    /**
     * Returns a fixed asset.
     */
    @Override
    public void getFixedAssetApprovalStatus(GetFixedAssetApprovalStatusRequest request, StreamObserver<GetFixedAssetApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFixedAssetApprovalStatus(request));
    }

    /**
     * Comments a fixed asset.
     */
    @Override
    public void commentFixedAssetApproval(CommentFixedAssetApprovalRequest request, StreamObserver<CommentFixedAssetApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentFixedAssetApproval(request));
    }
}
