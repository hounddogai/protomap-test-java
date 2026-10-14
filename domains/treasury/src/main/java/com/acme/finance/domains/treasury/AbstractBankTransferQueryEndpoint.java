package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.BankTransferQueryServiceGrpc;
import com.acme.treasury.v1.BrowseBankTransfersRequest;
import com.acme.treasury.v1.BrowseBankTransfersResponse;
import com.acme.treasury.v1.CountBankTransfersRequest;
import com.acme.treasury.v1.CountBankTransfersResponse;
import com.acme.treasury.v1.FindBankTransfersRequest;
import com.acme.treasury.v1.FindBankTransfersResponse;
import com.acme.treasury.v1.LookupBankTransferRequest;
import com.acme.treasury.v1.LookupBankTransferResponse;
import com.acme.treasury.v1.PeekBankTransferRequest;
import com.acme.treasury.v1.PeekBankTransferResponse;
import com.acme.treasury.v1.ResolveBankTransfersRequest;
import com.acme.treasury.v1.ResolveBankTransfersResponse;
import com.acme.treasury.v1.SuggestBankTransfersRequest;
import com.acme.treasury.v1.SuggestBankTransfersResponse;
import io.grpc.stub.StreamObserver;

/**
 * Answers read-only questions about bank transfers, for screens and reports.
 */
public abstract class AbstractBankTransferQueryEndpoint extends BankTransferQueryServiceGrpc.BankTransferQueryServiceImplBase {
    protected final BankTransferQueryApplication application;

    protected AbstractBankTransferQueryEndpoint(BankTransferQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a bank transfer.
     */
    @Override
    public void lookupBankTransfer(LookupBankTransferRequest request, StreamObserver<LookupBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupBankTransfer(request));
    }

    /**
     * Lists bank transfers a page at a time.
     */
    @Override
    public void browseBankTransfers(BrowseBankTransfersRequest request, StreamObserver<BrowseBankTransfersResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseBankTransfers(request));
    }

    /**
     * Lists bank transfers a page at a time.
     */
    @Override
    public void findBankTransfers(FindBankTransfersRequest request, StreamObserver<FindBankTransfersResponse> observer) {
        RpcSupport.unary(observer, () -> application.findBankTransfers(request));
    }

    /**
     * Counts bank transfers by status.
     */
    @Override
    public void countBankTransfers(CountBankTransfersRequest request, StreamObserver<CountBankTransfersResponse> observer) {
        RpcSupport.unary(observer, () -> application.countBankTransfers(request));
    }

    /**
     * Returns several bank transfers at once.
     */
    @Override
    public void resolveBankTransfers(ResolveBankTransfersRequest request, StreamObserver<ResolveBankTransfersResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveBankTransfers(request));
    }

    /**
     * Lists bank transfers a page at a time.
     */
    @Override
    public void suggestBankTransfers(SuggestBankTransfersRequest request, StreamObserver<SuggestBankTransfersResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestBankTransfers(request));
    }

    /**
     * Returns a bank transfer.
     */
    @Override
    public void peekBankTransfer(PeekBankTransferRequest request, StreamObserver<PeekBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekBankTransfer(request));
    }
}
