package com.acme.finance.domains.banking;

import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BrowseBankAccountsRequest;
import com.acme.banking.v1.BrowseBankAccountsResponse;
import com.acme.banking.v1.CountBankAccountsRequest;
import com.acme.banking.v1.CountBankAccountsResponse;
import com.acme.banking.v1.ExportBankAccountsRequest;
import com.acme.banking.v1.ExportBankAccountsResponse;
import com.acme.banking.v1.FindBankAccountsRequest;
import com.acme.banking.v1.FindBankAccountsResponse;
import com.acme.banking.v1.LookupBankAccountRequest;
import com.acme.banking.v1.LookupBankAccountResponse;
import com.acme.banking.v1.PeekBankAccountRequest;
import com.acme.banking.v1.PeekBankAccountResponse;
import com.acme.banking.v1.ResolveBankAccountsRequest;
import com.acme.banking.v1.ResolveBankAccountsResponse;
import com.acme.banking.v1.SuggestBankAccountsRequest;
import com.acme.banking.v1.SuggestBankAccountsResponse;
import com.acme.banking.v1.SummarizeBankAccountsRequest;
import com.acme.banking.v1.SummarizeBankAccountsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about bank accounts, for screens and reports.
 */
@GrpcService
public class BankAccountQueryEndpoint extends BankAccountQueryServiceGrpc.BankAccountQueryServiceImplBase {
    protected final BankAccountQueryApplication application;

    public BankAccountQueryEndpoint(BankAccountQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a bank account.
     */
    @Override
    public void lookupBankAccount(LookupBankAccountRequest request, StreamObserver<LookupBankAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupBankAccount(request));
    }

    /**
     * Lists bank accounts a page at a time.
     */
    @Override
    public void browseBankAccounts(BrowseBankAccountsRequest request, StreamObserver<BrowseBankAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseBankAccounts(request));
    }

    /**
     * Lists bank accounts a page at a time.
     */
    @Override
    public void findBankAccounts(FindBankAccountsRequest request, StreamObserver<FindBankAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findBankAccounts(request));
    }

    /**
     * Counts bank accounts by status.
     */
    @Override
    public void countBankAccounts(CountBankAccountsRequest request, StreamObserver<CountBankAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countBankAccounts(request));
    }

    /**
     * Returns several bank accounts at once.
     */
    @Override
    public void resolveBankAccounts(ResolveBankAccountsRequest request, StreamObserver<ResolveBankAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveBankAccounts(request));
    }

    /**
     * Streams every matching bank account.
     */
    @Override
    public void exportBankAccounts(ExportBankAccountsRequest request, StreamObserver<ExportBankAccountsResponse> observer) {
        try {
            var snapshot = application.exportBankAccounts(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a bank account.
     */
    @Override
    public void summarizeBankAccounts(SummarizeBankAccountsRequest request, StreamObserver<SummarizeBankAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeBankAccounts(request));
    }

    /**
     * Lists bank accounts a page at a time.
     */
    @Override
    public void suggestBankAccounts(SuggestBankAccountsRequest request, StreamObserver<SuggestBankAccountsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestBankAccounts(request));
    }

    /**
     * Returns a bank account.
     */
    @Override
    public void peekBankAccount(PeekBankAccountRequest request, StreamObserver<PeekBankAccountResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekBankAccount(request));
    }
}
