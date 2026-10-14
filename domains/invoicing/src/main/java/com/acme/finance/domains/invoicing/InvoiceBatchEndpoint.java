package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesRequest;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesResponse;
import com.acme.invoicing.v1.CloneInvoiceBatchRequest;
import com.acme.invoicing.v1.CloneInvoiceBatchResponse;
import com.acme.invoicing.v1.CreateInvoiceBatchRequest;
import com.acme.invoicing.v1.CreateInvoiceBatchResponse;
import com.acme.invoicing.v1.DeleteInvoiceBatchRequest;
import com.acme.invoicing.v1.DeleteInvoiceBatchResponse;
import com.acme.invoicing.v1.GetInvoiceBatchRequest;
import com.acme.invoicing.v1.GetInvoiceBatchResponse;
import com.acme.invoicing.v1.InvoiceBatchServiceGrpc;
import com.acme.invoicing.v1.ListInvoiceBatchesRequest;
import com.acme.invoicing.v1.ListInvoiceBatchesResponse;
import com.acme.invoicing.v1.UpdateInvoiceBatchRequest;
import com.acme.invoicing.v1.UpdateInvoiceBatchResponse;
import com.acme.invoicing.v1.WatchInvoiceBatchesRequest;
import com.acme.invoicing.v1.WatchInvoiceBatchesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages invoice batches: creates, reads, changes, and lists them.
 */
@GrpcService
public class InvoiceBatchEndpoint extends InvoiceBatchServiceGrpc.InvoiceBatchServiceImplBase {
    protected final InvoiceBatchApplication application;

    public InvoiceBatchEndpoint(InvoiceBatchApplication application) {
        this.application = application;
    }

    /**
     * Creates an invoice batch.
     */
    @Override
    public void createInvoiceBatch(CreateInvoiceBatchRequest request, StreamObserver<CreateInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.createInvoiceBatch(request));
    }

    /**
     * Returns an invoice batch.
     */
    @Override
    public void getInvoiceBatch(GetInvoiceBatchRequest request, StreamObserver<GetInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.getInvoiceBatch(request));
    }

    /**
     * Changes an invoice batch.
     */
    @Override
    public void updateInvoiceBatch(UpdateInvoiceBatchRequest request, StreamObserver<UpdateInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateInvoiceBatch(request));
    }

    /**
     * Deletes an invoice batch.
     */
    @Override
    public void deleteInvoiceBatch(DeleteInvoiceBatchRequest request, StreamObserver<DeleteInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteInvoiceBatch(request));
    }

    /**
     * Lists invoice batches a page at a time.
     */
    @Override
    public void listInvoiceBatches(ListInvoiceBatchesRequest request, StreamObserver<ListInvoiceBatchesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listInvoiceBatches(request));
    }

    /**
     * Streams the changes of invoice batches as they happen.
     */
    @Override
    public void watchInvoiceBatches(WatchInvoiceBatchesRequest request, StreamObserver<WatchInvoiceBatchesResponse> observer) {
        try {
            var snapshot = application.watchInvoiceBatches(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of invoice batches and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateInvoiceBatchesRequest> batchCreateInvoiceBatches(StreamObserver<BatchCreateInvoiceBatchesResponse> observer) {
        return new StreamObserver<BatchCreateInvoiceBatchesRequest>() {
            private boolean finished;
            private final BatchCreateInvoiceBatchesResponse.Builder summary = BatchCreateInvoiceBatchesResponse.newBuilder();

            @Override
            public void onNext(BatchCreateInvoiceBatchesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateInvoiceBatches(request);
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
     * Creates an invoice batch.
     */
    @Override
    public void cloneInvoiceBatch(CloneInvoiceBatchRequest request, StreamObserver<CloneInvoiceBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneInvoiceBatch(request));
    }
}
