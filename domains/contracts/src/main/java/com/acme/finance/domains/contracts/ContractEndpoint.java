package com.acme.finance.domains.contracts;

import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.BatchGetContractsRequest;
import com.acme.contracts.v1.BatchGetContractsResponse;
import com.acme.contracts.v1.CloneContractRequest;
import com.acme.contracts.v1.CloneContractResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.contracts.v1.CreateContractRequest;
import com.acme.contracts.v1.CreateContractResponse;
import com.acme.contracts.v1.GetContractRequest;
import com.acme.contracts.v1.GetContractResponse;
import com.acme.contracts.v1.ListContractsRequest;
import com.acme.contracts.v1.ListContractsResponse;
import com.acme.contracts.v1.SearchContractsRequest;
import com.acme.contracts.v1.SearchContractsResponse;
import com.acme.contracts.v1.UpdateContractRequest;
import com.acme.contracts.v1.UpdateContractResponse;
import com.acme.contracts.v1.WatchContractsRequest;
import com.acme.contracts.v1.WatchContractsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages contracts: creates, reads, changes, and lists them.
 */
@GrpcService
public class ContractEndpoint extends ContractServiceGrpc.ContractServiceImplBase {
    protected final ContractApplication application;

    public ContractEndpoint(ContractApplication application) {
        this.application = application;
    }

    /**
     * Creates a contract.
     */
    @Override
    public void createContract(CreateContractRequest request, StreamObserver<CreateContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.createContract(request));
    }

    /**
     * Returns a contract.
     */
    @Override
    public void getContract(GetContractRequest request, StreamObserver<GetContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.getContract(request));
    }

    /**
     * Changes a contract.
     */
    @Override
    public void updateContract(UpdateContractRequest request, StreamObserver<UpdateContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateContract(request));
    }

    /**
     * Lists contracts a page at a time.
     */
    @Override
    public void listContracts(ListContractsRequest request, StreamObserver<ListContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listContracts(request));
    }

    /**
     * Returns several contracts at once.
     */
    @Override
    public void batchGetContracts(BatchGetContractsRequest request, StreamObserver<BatchGetContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetContracts(request));
    }

    /**
     * Lists contracts a page at a time.
     */
    @Override
    public void searchContracts(SearchContractsRequest request, StreamObserver<SearchContractsResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchContracts(request));
    }

    /**
     * Streams the changes of contracts as they happen.
     */
    @Override
    public void watchContracts(WatchContractsRequest request, StreamObserver<WatchContractsResponse> observer) {
        try {
            var snapshot = application.watchContracts(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of contracts and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateContractsRequest> batchCreateContracts(StreamObserver<BatchCreateContractsResponse> observer) {
        return new StreamObserver<BatchCreateContractsRequest>() {
            private boolean finished;
            private final BatchCreateContractsResponse.Builder summary = BatchCreateContractsResponse.newBuilder();

            @Override
            public void onNext(BatchCreateContractsRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateContracts(request);
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
     * Creates a contract.
     */
    @Override
    public void cloneContract(CloneContractRequest request, StreamObserver<CloneContractResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneContract(request));
    }
}
