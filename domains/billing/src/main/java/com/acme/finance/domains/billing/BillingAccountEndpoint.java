package com.acme.finance.domains.billing;

import com.acme.billing.v1.BatchCreateBillingAccountsRequest;
import com.acme.billing.v1.BatchCreateBillingAccountsResponse;
import com.acme.billing.v1.BatchGetBillingAccountsRequest;
import com.acme.billing.v1.BatchGetBillingAccountsResponse;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.billing.v1.CloneBillingAccountRequest;
import com.acme.billing.v1.CloneBillingAccountResponse;
import com.acme.billing.v1.CreateBillingAccountRequest;
import com.acme.billing.v1.CreateBillingAccountResponse;
import com.acme.billing.v1.GetBillingAccountRequest;
import com.acme.billing.v1.GetBillingAccountResponse;
import com.acme.billing.v1.ListBillingAccountsRequest;
import com.acme.billing.v1.ListBillingAccountsResponse;
import com.acme.billing.v1.UpdateBillingAccountRequest;
import com.acme.billing.v1.UpdateBillingAccountResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages billing accounts: creates, reads, changes, and lists them.
 */
@GrpcService
public class BillingAccountEndpoint extends BillingAccountServiceGrpc.BillingAccountServiceImplBase {
    protected final BillingAccountApplication application;

    public BillingAccountEndpoint(BillingAccountApplication application) {
        this.application = application;
    }

    /**
     * Creates a billing account.
     */
    @Override
    public void createBillingAccount(CreateBillingAccountRequest request, StreamObserver<CreateBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.createBillingAccount(request));
    }

    /**
     * Returns a billing account.
     */
    @Override
    public void getBillingAccount(GetBillingAccountRequest request, StreamObserver<GetBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.getBillingAccount(request));
    }

    /**
     * Changes a billing account.
     */
    @Override
    public void updateBillingAccount(UpdateBillingAccountRequest request, StreamObserver<UpdateBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateBillingAccount(request));
    }

    /**
     * Lists billing accounts a page at a time.
     */
    @Override
    public void listBillingAccounts(ListBillingAccountsRequest request, StreamObserver<ListBillingAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listBillingAccounts(request));
    }

    /**
     * Returns several billing accounts at once.
     */
    @Override
    public void batchGetBillingAccounts(BatchGetBillingAccountsRequest request, StreamObserver<BatchGetBillingAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetBillingAccounts(request));
    }

    /**
     * Takes a stream of billing accounts and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateBillingAccountsRequest> batchCreateBillingAccounts(StreamObserver<BatchCreateBillingAccountsResponse> observer) {
        return new StreamObserver<BatchCreateBillingAccountsRequest>() {
            private boolean finished;
            private final BatchCreateBillingAccountsResponse.Builder summary = BatchCreateBillingAccountsResponse.newBuilder();

            @Override
            public void onNext(BatchCreateBillingAccountsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateBillingAccounts(request);
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
     * Creates a billing account.
     */
    @Override
    public void cloneBillingAccount(CloneBillingAccountRequest request, StreamObserver<CloneBillingAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneBillingAccount(request));
    }
}
