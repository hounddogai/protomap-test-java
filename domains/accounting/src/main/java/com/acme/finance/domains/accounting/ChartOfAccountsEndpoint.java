package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.BatchCreateChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesResponse;
import com.acme.accounting.v1.BatchGetChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchGetChartOfAccountsesResponse;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.accounting.v1.CreateChartOfAccountsRequest;
import com.acme.accounting.v1.CreateChartOfAccountsResponse;
import com.acme.accounting.v1.DeleteChartOfAccountsRequest;
import com.acme.accounting.v1.DeleteChartOfAccountsResponse;
import com.acme.accounting.v1.GetChartOfAccountsRequest;
import com.acme.accounting.v1.GetChartOfAccountsResponse;
import com.acme.accounting.v1.ListChartOfAccountsesRequest;
import com.acme.accounting.v1.ListChartOfAccountsesResponse;
import com.acme.accounting.v1.UpdateChartOfAccountsRequest;
import com.acme.accounting.v1.UpdateChartOfAccountsResponse;
import com.acme.accounting.v1.WatchChartOfAccountsesRequest;
import com.acme.accounting.v1.WatchChartOfAccountsesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages chart of accountses: creates, reads, changes, and lists them.
 */
@GrpcService
public class ChartOfAccountsEndpoint extends ChartOfAccountsServiceGrpc.ChartOfAccountsServiceImplBase {
    protected final ChartOfAccountsApplication application;

    public ChartOfAccountsEndpoint(ChartOfAccountsApplication application) {
        this.application = application;
    }

    /**
     * Creates a chart of accounts.
     */
    @Override
    public void createChartOfAccounts(CreateChartOfAccountsRequest request, StreamObserver<CreateChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.createChartOfAccounts(request));
    }

    /**
     * Returns a chart of accounts.
     */
    @Override
    public void getChartOfAccounts(GetChartOfAccountsRequest request, StreamObserver<GetChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.getChartOfAccounts(request));
    }

    /**
     * Changes a chart of accounts.
     */
    @Override
    public void updateChartOfAccounts(UpdateChartOfAccountsRequest request, StreamObserver<UpdateChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateChartOfAccounts(request));
    }

    /**
     * Deletes a chart of accounts.
     */
    @Override
    public void deleteChartOfAccounts(DeleteChartOfAccountsRequest request, StreamObserver<DeleteChartOfAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteChartOfAccounts(request));
    }

    /**
     * Lists chart of accountses a page at a time.
     */
    @Override
    public void listChartOfAccountses(ListChartOfAccountsesRequest request, StreamObserver<ListChartOfAccountsesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listChartOfAccountses(request));
    }

    /**
     * Returns several chart of accountses at once.
     */
    @Override
    public void batchGetChartOfAccountses(BatchGetChartOfAccountsesRequest request, StreamObserver<BatchGetChartOfAccountsesResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetChartOfAccountses(request));
    }

    /**
     * Streams the changes of chart of accountses as they happen.
     */
    @Override
    public void watchChartOfAccountses(WatchChartOfAccountsesRequest request, StreamObserver<WatchChartOfAccountsesResponse> observer) {
        try {
            var snapshot = application.watchChartOfAccountses(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of chart of accountses and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateChartOfAccountsesRequest> batchCreateChartOfAccountses(StreamObserver<BatchCreateChartOfAccountsesResponse> observer) {
        return new StreamObserver<BatchCreateChartOfAccountsesRequest>() {
            private boolean finished;
            private final BatchCreateChartOfAccountsesResponse.Builder summary = BatchCreateChartOfAccountsesResponse.newBuilder();

            @Override
            public void onNext(BatchCreateChartOfAccountsesRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateChartOfAccountses(request);
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
