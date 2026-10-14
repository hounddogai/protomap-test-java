package com.acme.finance.domains.payroll;

import com.acme.finance.platform.RpcSupport;
import com.acme.payroll.v1.BatchCreatePayRunsRequest;
import com.acme.payroll.v1.BatchCreatePayRunsResponse;
import com.acme.payroll.v1.BatchGetPayRunsRequest;
import com.acme.payroll.v1.BatchGetPayRunsResponse;
import com.acme.payroll.v1.ClonePayRunRequest;
import com.acme.payroll.v1.ClonePayRunResponse;
import com.acme.payroll.v1.CreatePayRunRequest;
import com.acme.payroll.v1.CreatePayRunResponse;
import com.acme.payroll.v1.GetPayRunRequest;
import com.acme.payroll.v1.GetPayRunResponse;
import com.acme.payroll.v1.ListPayRunsRequest;
import com.acme.payroll.v1.ListPayRunsResponse;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.payroll.v1.SearchPayRunsRequest;
import com.acme.payroll.v1.SearchPayRunsResponse;
import com.acme.payroll.v1.UpdatePayRunRequest;
import com.acme.payroll.v1.UpdatePayRunResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages pay runs: creates, reads, changes, and lists them.
 */
@GrpcService
public class PayRunEndpoint extends PayRunServiceGrpc.PayRunServiceImplBase {
    protected final PayRunApplication application;

    public PayRunEndpoint(PayRunApplication application) {
        this.application = application;
    }

    /**
     * Creates a pay run.
     */
    @Override
    public void createPayRun(CreatePayRunRequest request, StreamObserver<CreatePayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.createPayRun(request));
    }

    /**
     * Returns a pay run.
     */
    @Override
    public void getPayRun(GetPayRunRequest request, StreamObserver<GetPayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPayRun(request));
    }

    /**
     * Changes a pay run.
     */
    @Override
    public void updatePayRun(UpdatePayRunRequest request, StreamObserver<UpdatePayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.updatePayRun(request));
    }

    /**
     * Lists pay runs a page at a time.
     */
    @Override
    public void listPayRuns(ListPayRunsRequest request, StreamObserver<ListPayRunsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPayRuns(request));
    }

    /**
     * Returns several pay runs at once.
     */
    @Override
    public void batchGetPayRuns(BatchGetPayRunsRequest request, StreamObserver<BatchGetPayRunsResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetPayRuns(request));
    }

    /**
     * Lists pay runs a page at a time.
     */
    @Override
    public void searchPayRuns(SearchPayRunsRequest request, StreamObserver<SearchPayRunsResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchPayRuns(request));
    }

    /**
     * Takes a stream of pay runs and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreatePayRunsRequest> batchCreatePayRuns(StreamObserver<BatchCreatePayRunsResponse> observer) {
        return new StreamObserver<BatchCreatePayRunsRequest>() {
            private boolean finished;
            private final BatchCreatePayRunsResponse.Builder summary = BatchCreatePayRunsResponse.newBuilder();

            @Override
            public void onNext(BatchCreatePayRunsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreatePayRuns(request);
                    summary.mergeFrom(response);
                } catch (RuntimeException failure) {
                    finished = true;
                    observer.onError(RpcSupport.status(failure));
                }
            }

            @Override
            public void onError(Throwable failure) {
                if (!finished) {
                    finished = true;
                    observer.onError(RpcSupport.status(failure));
                }
            }

            @Override
            public void onCompleted() {
                if (finished) { return; }
                finished = true;
                observer.onNext(summary.build());
                observer.onCompleted();
            }
        };
    }

    /**
     * Creates a pay run.
     */
    @Override
    public void clonePayRun(ClonePayRunRequest request, StreamObserver<ClonePayRunResponse> observer) {
        RpcSupport.unary(observer, () -> application.clonePayRun(request));
    }
}
