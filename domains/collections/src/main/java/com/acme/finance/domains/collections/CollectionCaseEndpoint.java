package com.acme.finance.domains.collections;

import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CloneCollectionCaseRequest;
import com.acme.collections.v1.CloneCollectionCaseResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.CreateCollectionCaseRequest;
import com.acme.collections.v1.CreateCollectionCaseResponse;
import com.acme.collections.v1.DeleteCollectionCaseRequest;
import com.acme.collections.v1.DeleteCollectionCaseResponse;
import com.acme.collections.v1.GetCollectionCaseRequest;
import com.acme.collections.v1.GetCollectionCaseResponse;
import com.acme.collections.v1.UpdateCollectionCaseRequest;
import com.acme.collections.v1.UpdateCollectionCaseResponse;
import com.acme.collections.v1.WatchCollectionCasesRequest;
import com.acme.collections.v1.WatchCollectionCasesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages collection cases: creates, reads, changes, and lists them.
 */
@GrpcService
public class CollectionCaseEndpoint extends CollectionCaseServiceGrpc.CollectionCaseServiceImplBase {
    protected final CollectionCaseApplication application;

    public CollectionCaseEndpoint(CollectionCaseApplication application) {
        this.application = application;
    }

    /**
     * Creates a collection case.
     */
    @Override
    public void createCollectionCase(CreateCollectionCaseRequest request, StreamObserver<CreateCollectionCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.createCollectionCase(request));
    }

    /**
     * Returns a collection case.
     */
    @Override
    public void getCollectionCase(GetCollectionCaseRequest request, StreamObserver<GetCollectionCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCollectionCase(request));
    }

    /**
     * Changes a collection case.
     */
    @Override
    public void updateCollectionCase(UpdateCollectionCaseRequest request, StreamObserver<UpdateCollectionCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateCollectionCase(request));
    }

    /**
     * Deletes a collection case.
     */
    @Override
    public void deleteCollectionCase(DeleteCollectionCaseRequest request, StreamObserver<DeleteCollectionCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteCollectionCase(request));
    }

    /**
     * Streams the changes of collection cases as they happen.
     */
    @Override
    public void watchCollectionCases(WatchCollectionCasesRequest request, StreamObserver<WatchCollectionCasesResponse> observer) {
        try {
            var snapshot = application.watchCollectionCases(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of collection cases and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateCollectionCasesRequest> batchCreateCollectionCases(StreamObserver<BatchCreateCollectionCasesResponse> observer) {
        return new StreamObserver<BatchCreateCollectionCasesRequest>() {
            private boolean finished;
            private final BatchCreateCollectionCasesResponse.Builder summary = BatchCreateCollectionCasesResponse.newBuilder();

            @Override
            public void onNext(BatchCreateCollectionCasesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateCollectionCases(request);
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
     * Creates a collection case.
     */
    @Override
    public void cloneCollectionCase(CloneCollectionCaseRequest request, StreamObserver<CloneCollectionCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneCollectionCase(request));
    }
}
