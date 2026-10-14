package com.acme.finance.domains.suppliers;

import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.BatchCreateSuppliersRequest;
import com.acme.suppliers.v1.BatchCreateSuppliersResponse;
import com.acme.suppliers.v1.BatchGetSuppliersRequest;
import com.acme.suppliers.v1.BatchGetSuppliersResponse;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.DeleteSupplierRequest;
import com.acme.suppliers.v1.DeleteSupplierResponse;
import com.acme.suppliers.v1.GetSupplierRequest;
import com.acme.suppliers.v1.GetSupplierResponse;
import com.acme.suppliers.v1.ListSuppliersRequest;
import com.acme.suppliers.v1.ListSuppliersResponse;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.UpdateSupplierRequest;
import com.acme.suppliers.v1.UpdateSupplierResponse;
import com.acme.suppliers.v1.WatchSuppliersRequest;
import com.acme.suppliers.v1.WatchSuppliersResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages suppliers: creates, reads, changes, and lists them.
 */
@GrpcService
public class SupplierEndpoint extends SupplierServiceGrpc.SupplierServiceImplBase {
    protected final SupplierApplication application;

    public SupplierEndpoint(SupplierApplication application) {
        this.application = application;
    }

    /**
     * Creates a supplier.
     */
    @Override
    public void createSupplier(CreateSupplierRequest request, StreamObserver<CreateSupplierResponse> observer) {
        RpcSupport.unary(observer, () -> application.createSupplier(request));
    }

    /**
     * Returns a supplier.
     */
    @Override
    public void getSupplier(GetSupplierRequest request, StreamObserver<GetSupplierResponse> observer) {
        RpcSupport.unary(observer, () -> application.getSupplier(request));
    }

    /**
     * Changes a supplier.
     */
    @Override
    public void updateSupplier(UpdateSupplierRequest request, StreamObserver<UpdateSupplierResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateSupplier(request));
    }

    /**
     * Deletes a supplier.
     */
    @Override
    public void deleteSupplier(DeleteSupplierRequest request, StreamObserver<DeleteSupplierResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteSupplier(request));
    }

    /**
     * Lists suppliers a page at a time.
     */
    @Override
    public void listSuppliers(ListSuppliersRequest request, StreamObserver<ListSuppliersResponse> observer) {
        RpcSupport.unary(observer, () -> application.listSuppliers(request));
    }

    /**
     * Returns several suppliers at once.
     */
    @Override
    public void batchGetSuppliers(BatchGetSuppliersRequest request, StreamObserver<BatchGetSuppliersResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetSuppliers(request));
    }

    /**
     * Streams the changes of suppliers as they happen.
     */
    @Override
    public void watchSuppliers(WatchSuppliersRequest request, StreamObserver<WatchSuppliersResponse> observer) {
        try {
            var snapshot = application.watchSuppliers(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of suppliers and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateSuppliersRequest> batchCreateSuppliers(StreamObserver<BatchCreateSuppliersResponse> observer) {
        return new StreamObserver<BatchCreateSuppliersRequest>() {
            private boolean finished;
            private final BatchCreateSuppliersResponse.Builder summary = BatchCreateSuppliersResponse.newBuilder();

            @Override
            public void onNext(BatchCreateSuppliersRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateSuppliers(request);
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
}
