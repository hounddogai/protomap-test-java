package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.ApproveStressTestRequest;
import com.acme.risk.v1.ApproveStressTestResponse;
import com.acme.risk.v1.CommentStressTestApprovalRequest;
import com.acme.risk.v1.CommentStressTestApprovalResponse;
import com.acme.risk.v1.DelegateStressTestApprovalRequest;
import com.acme.risk.v1.DelegateStressTestApprovalResponse;
import com.acme.risk.v1.EscalateStressTestRequest;
import com.acme.risk.v1.EscalateStressTestResponse;
import com.acme.risk.v1.GetStressTestApprovalStatusRequest;
import com.acme.risk.v1.GetStressTestApprovalStatusResponse;
import com.acme.risk.v1.RecallStressTestRequest;
import com.acme.risk.v1.RecallStressTestResponse;
import com.acme.risk.v1.RejectStressTestRequest;
import com.acme.risk.v1.RejectStressTestResponse;
import com.acme.risk.v1.StressTestApprovalServiceGrpc;
import com.acme.risk.v1.SubmitStressTestRequest;
import com.acme.risk.v1.SubmitStressTestResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Routes stress tests through approval, with escalation and delegation.
 */
@GrpcService
public class StressTestApprovalEndpoint extends StressTestApprovalServiceGrpc.StressTestApprovalServiceImplBase {
    protected final StressTestApprovalApplication application;

    public StressTestApprovalEndpoint(StressTestApprovalApplication application) {
        this.application = application;
    }

    /**
     * Submits a stress test.
     */
    @Override
    public void submitStressTest(SubmitStressTestRequest request, StreamObserver<SubmitStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.submitStressTest(request));
    }

    /**
     * Approves a stress test.
     */
    @Override
    public void approveStressTest(ApproveStressTestRequest request, StreamObserver<ApproveStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.approveStressTest(request));
    }

    /**
     * Rejects a stress test.
     */
    @Override
    public void rejectStressTest(RejectStressTestRequest request, StreamObserver<RejectStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.rejectStressTest(request));
    }

    /**
     * Escalates a stress test.
     */
    @Override
    public void escalateStressTest(EscalateStressTestRequest request, StreamObserver<EscalateStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.escalateStressTest(request));
    }

    /**
     * Delegates a stress test.
     */
    @Override
    public void delegateStressTestApproval(DelegateStressTestApprovalRequest request, StreamObserver<DelegateStressTestApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.delegateStressTestApproval(request));
    }

    /**
     * Recalls a stress test.
     */
    @Override
    public void recallStressTest(RecallStressTestRequest request, StreamObserver<RecallStressTestResponse> observer) {
        RpcSupport.unary(observer, () -> application.recallStressTest(request));
    }

    /**
     * Returns a stress test.
     */
    @Override
    public void getStressTestApprovalStatus(GetStressTestApprovalStatusRequest request, StreamObserver<GetStressTestApprovalStatusResponse> observer) {
        RpcSupport.unary(observer, () -> application.getStressTestApprovalStatus(request));
    }

    /**
     * Comments a stress test.
     */
    @Override
    public void commentStressTestApproval(CommentStressTestApprovalRequest request, StreamObserver<CommentStressTestApprovalResponse> observer) {
        RpcSupport.unary(observer, () -> application.commentStressTestApproval(request));
    }
}
