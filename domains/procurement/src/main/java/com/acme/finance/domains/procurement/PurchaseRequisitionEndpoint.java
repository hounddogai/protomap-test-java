package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.BatchGetPurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchGetPurchaseRequisitionsResponse;
import com.acme.procurement.v1.ClonePurchaseRequisitionRequest;
import com.acme.procurement.v1.ClonePurchaseRequisitionResponse;
import com.acme.procurement.v1.CreatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CreatePurchaseRequisitionResponse;
import com.acme.procurement.v1.DeletePurchaseRequisitionRequest;
import com.acme.procurement.v1.DeletePurchaseRequisitionResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionResponse;
import com.acme.procurement.v1.ListPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ListPurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.procurement.v1.SearchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.SearchPurchaseRequisitionsResponse;
import com.acme.procurement.v1.UpdatePurchaseRequisitionRequest;
import com.acme.procurement.v1.UpdatePurchaseRequisitionResponse;
import com.acme.procurement.v1.WatchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.WatchPurchaseRequisitionsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages purchase requisitions: creates, reads, changes, and lists them.
 */
@GrpcService
public class PurchaseRequisitionEndpoint extends PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceImplBase {
    protected final PurchaseRequisitionApplication application;

    public PurchaseRequisitionEndpoint(PurchaseRequisitionApplication application) {
        this.application = application;
    }

    /**
     * Creates a purchase requisition.
     */
    @Override
    public void createPurchaseRequisition(CreatePurchaseRequisitionRequest request, StreamObserver<CreatePurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.createPurchaseRequisition(request));
    }

    /**
     * Returns a purchase requisition.
     */
    @Override
    public void getPurchaseRequisition(GetPurchaseRequisitionRequest request, StreamObserver<GetPurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPurchaseRequisition(request));
    }

    /**
     * Changes a purchase requisition.
     */
    @Override
    public void updatePurchaseRequisition(UpdatePurchaseRequisitionRequest request, StreamObserver<UpdatePurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.updatePurchaseRequisition(request));
    }

    /**
     * Deletes a purchase requisition.
     */
    @Override
    public void deletePurchaseRequisition(DeletePurchaseRequisitionRequest request, StreamObserver<DeletePurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.deletePurchaseRequisition(request));
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    @Override
    public void listPurchaseRequisitions(ListPurchaseRequisitionsRequest request, StreamObserver<ListPurchaseRequisitionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPurchaseRequisitions(request));
    }

    /**
     * Returns several purchase requisitions at once.
     */
    @Override
    public void batchGetPurchaseRequisitions(BatchGetPurchaseRequisitionsRequest request, StreamObserver<BatchGetPurchaseRequisitionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetPurchaseRequisitions(request));
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    @Override
    public void searchPurchaseRequisitions(SearchPurchaseRequisitionsRequest request, StreamObserver<SearchPurchaseRequisitionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchPurchaseRequisitions(request));
    }

    /**
     * Streams the changes of purchase requisitions as they happen.
     */
    @Override
    public void watchPurchaseRequisitions(WatchPurchaseRequisitionsRequest request, StreamObserver<WatchPurchaseRequisitionsResponse> observer) {
        try {
            var snapshot = application.watchPurchaseRequisitions(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of purchase requisitions and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreatePurchaseRequisitionsRequest> batchCreatePurchaseRequisitions(StreamObserver<BatchCreatePurchaseRequisitionsResponse> observer) {
        return new StreamObserver<BatchCreatePurchaseRequisitionsRequest>() {
            private boolean finished;
            private final BatchCreatePurchaseRequisitionsResponse.Builder summary = BatchCreatePurchaseRequisitionsResponse.newBuilder();

            @Override
            public void onNext(BatchCreatePurchaseRequisitionsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreatePurchaseRequisitions(request);
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
     * Creates a purchase requisition.
     */
    @Override
    public void clonePurchaseRequisition(ClonePurchaseRequisitionRequest request, StreamObserver<ClonePurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.clonePurchaseRequisition(request));
    }
}
