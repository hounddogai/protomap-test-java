package com.acme.finance.domains.banking;

import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.banking.v1.BatchGetBankAccountsRequest;
import com.acme.banking.v1.BatchGetBankAccountsResponse;
import com.acme.banking.v1.CloneBankAccountRequest;
import com.acme.banking.v1.CloneBankAccountResponse;
import com.acme.banking.v1.CreateBankAccountRequest;
import com.acme.banking.v1.CreateBankAccountResponse;
import com.acme.banking.v1.DeleteBankAccountRequest;
import com.acme.banking.v1.DeleteBankAccountResponse;
import com.acme.banking.v1.GetBankAccountRequest;
import com.acme.banking.v1.GetBankAccountResponse;
import com.acme.banking.v1.ListBankAccountsRequest;
import com.acme.banking.v1.ListBankAccountsResponse;
import com.acme.banking.v1.SearchBankAccountsRequest;
import com.acme.banking.v1.SearchBankAccountsResponse;
import com.acme.banking.v1.UpdateBankAccountRequest;
import com.acme.banking.v1.UpdateBankAccountResponse;
import com.acme.banking.v1.WatchBankAccountsRequest;
import com.acme.banking.v1.WatchBankAccountsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages bank accounts: creates, reads, changes, and lists them.
 */
@GrpcService
public class BankAccountEndpoint extends BankAccountServiceGrpc.BankAccountServiceImplBase {
    protected final BankAccountApplication application;

    public BankAccountEndpoint(BankAccountApplication application) {
        this.application = application;
    }

    /**
     * Creates a bank account.
     */
    @Override
    public void createBankAccount(CreateBankAccountRequest request, StreamObserver<CreateBankAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.createBankAccount(request));
    }

    /**
     * Returns a bank account.
     */
    @Override
    public void getBankAccount(GetBankAccountRequest request, StreamObserver<GetBankAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.getBankAccount(request));
    }

    /**
     * Changes a bank account.
     */
    @Override
    public void updateBankAccount(UpdateBankAccountRequest request, StreamObserver<UpdateBankAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateBankAccount(request));
    }

    /**
     * Deletes a bank account.
     */
    @Override
    public void deleteBankAccount(DeleteBankAccountRequest request, StreamObserver<DeleteBankAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteBankAccount(request));
    }

    /**
     * Lists bank accounts a page at a time.
     */
    @Override
    public void listBankAccounts(ListBankAccountsRequest request, StreamObserver<ListBankAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listBankAccounts(request));
    }

    /**
     * Returns several bank accounts at once.
     */
    @Override
    public void batchGetBankAccounts(BatchGetBankAccountsRequest request, StreamObserver<BatchGetBankAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetBankAccounts(request));
    }

    /**
     * Lists bank accounts a page at a time.
     */
    @Override
    public void searchBankAccounts(SearchBankAccountsRequest request, StreamObserver<SearchBankAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchBankAccounts(request));
    }

    /**
     * Streams the changes of bank accounts as they happen.
     */
    @Override
    public void watchBankAccounts(WatchBankAccountsRequest request, StreamObserver<WatchBankAccountsResponse> observer) {
        try {
            var snapshot = application.watchBankAccounts(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of bank accounts and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateBankAccountsRequest> batchCreateBankAccounts(StreamObserver<BatchCreateBankAccountsResponse> observer) {
        return new StreamObserver<BatchCreateBankAccountsRequest>() {
            private boolean finished;
            private final BatchCreateBankAccountsResponse.Builder summary = BatchCreateBankAccountsResponse.newBuilder();

            @Override
            public void onNext(BatchCreateBankAccountsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateBankAccounts(request);
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
     * Creates a bank account.
     */
    @Override
    public void cloneBankAccount(CloneBankAccountRequest request, StreamObserver<CloneBankAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneBankAccount(request));
    }
}
