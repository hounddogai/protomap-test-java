package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.BatchCreateCashPositionsRequest;
import com.acme.treasury.v1.BatchCreateCashPositionsResponse;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.acme.treasury.v1.CloneCashPositionRequest;
import com.acme.treasury.v1.CloneCashPositionResponse;
import com.acme.treasury.v1.CreateCashPositionRequest;
import com.acme.treasury.v1.CreateCashPositionResponse;
import com.acme.treasury.v1.DeleteCashPositionRequest;
import com.acme.treasury.v1.DeleteCashPositionResponse;
import com.acme.treasury.v1.GetCashPositionRequest;
import com.acme.treasury.v1.GetCashPositionResponse;
import com.acme.treasury.v1.SearchCashPositionsRequest;
import com.acme.treasury.v1.SearchCashPositionsResponse;
import com.acme.treasury.v1.UpdateCashPositionRequest;
import com.acme.treasury.v1.UpdateCashPositionResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages cash positions: creates, reads, changes, and lists them.
 */
@GrpcService
public class CashPositionEndpoint extends CashPositionServiceGrpc.CashPositionServiceImplBase {
    protected final CashPositionApplication application;

    public CashPositionEndpoint(CashPositionApplication application) {
        this.application = application;
    }

    /**
     * Creates a cash position.
     */
    @Override
    public void createCashPosition(CreateCashPositionRequest request, StreamObserver<CreateCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.createCashPosition(request));
    }

    /**
     * Returns a cash position.
     */
    @Override
    public void getCashPosition(GetCashPositionRequest request, StreamObserver<GetCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCashPosition(request));
    }

    /**
     * Changes a cash position.
     */
    @Override
    public void updateCashPosition(UpdateCashPositionRequest request, StreamObserver<UpdateCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateCashPosition(request));
    }

    /**
     * Deletes a cash position.
     */
    @Override
    public void deleteCashPosition(DeleteCashPositionRequest request, StreamObserver<DeleteCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteCashPosition(request));
    }

    /**
     * Lists cash positions a page at a time.
     */
    @Override
    public void searchCashPositions(SearchCashPositionsRequest request, StreamObserver<SearchCashPositionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchCashPositions(request));
    }

    /**
     * Takes a stream of cash positions and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateCashPositionsRequest> batchCreateCashPositions(StreamObserver<BatchCreateCashPositionsResponse> observer) {
        return new StreamObserver<BatchCreateCashPositionsRequest>() {
            private boolean finished;
            private final BatchCreateCashPositionsResponse.Builder summary = BatchCreateCashPositionsResponse.newBuilder();

            @Override
            public void onNext(BatchCreateCashPositionsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateCashPositions(request);
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
     * Creates a cash position.
     */
    @Override
    public void cloneCashPosition(CloneCashPositionRequest request, StreamObserver<CloneCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneCashPosition(request));
    }
}
