package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.BatchGetRiskRegistersRequest;
import com.acme.risk.v1.BatchGetRiskRegistersResponse;
import com.acme.risk.v1.CloneRiskRegisterRequest;
import com.acme.risk.v1.CloneRiskRegisterResponse;
import com.acme.risk.v1.CreateRiskRegisterRequest;
import com.acme.risk.v1.CreateRiskRegisterResponse;
import com.acme.risk.v1.DeleteRiskRegisterRequest;
import com.acme.risk.v1.DeleteRiskRegisterResponse;
import com.acme.risk.v1.GetRiskRegisterRequest;
import com.acme.risk.v1.GetRiskRegisterResponse;
import com.acme.risk.v1.ListRiskRegistersRequest;
import com.acme.risk.v1.ListRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.SearchRiskRegistersRequest;
import com.acme.risk.v1.SearchRiskRegistersResponse;
import com.acme.risk.v1.UpdateRiskRegisterRequest;
import com.acme.risk.v1.UpdateRiskRegisterResponse;
import com.acme.risk.v1.WatchRiskRegistersRequest;
import com.acme.risk.v1.WatchRiskRegistersResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages risk registers: creates, reads, changes, and lists them.
 */
@GrpcService
public class RiskRegisterEndpoint extends RiskRegisterServiceGrpc.RiskRegisterServiceImplBase {
    protected final RiskRegisterApplication application;

    public RiskRegisterEndpoint(RiskRegisterApplication application) {
        this.application = application;
    }

    /**
     * Creates a risk register.
     */
    @Override
    public void createRiskRegister(CreateRiskRegisterRequest request, StreamObserver<CreateRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.createRiskRegister(request));
    }

    /**
     * Returns a risk register.
     */
    @Override
    public void getRiskRegister(GetRiskRegisterRequest request, StreamObserver<GetRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRiskRegister(request));
    }

    /**
     * Changes a risk register.
     */
    @Override
    public void updateRiskRegister(UpdateRiskRegisterRequest request, StreamObserver<UpdateRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.updateRiskRegister(request));
    }

    /**
     * Deletes a risk register.
     */
    @Override
    public void deleteRiskRegister(DeleteRiskRegisterRequest request, StreamObserver<DeleteRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.deleteRiskRegister(request));
    }

    /**
     * Lists risk registers a page at a time.
     */
    @Override
    public void listRiskRegisters(ListRiskRegistersRequest request, StreamObserver<ListRiskRegistersResponse> observer) {
        RpcSupport.unary(observer, () -> application.listRiskRegisters(request));
    }

    /**
     * Returns several risk registers at once.
     */
    @Override
    public void batchGetRiskRegisters(BatchGetRiskRegistersRequest request, StreamObserver<BatchGetRiskRegistersResponse> observer) {
        RpcSupport.unary(observer, () -> application.batchGetRiskRegisters(request));
    }

    /**
     * Lists risk registers a page at a time.
     */
    @Override
    public void searchRiskRegisters(SearchRiskRegistersRequest request, StreamObserver<SearchRiskRegistersResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchRiskRegisters(request));
    }

    /**
     * Streams the changes of risk registers as they happen.
     */
    @Override
    public void watchRiskRegisters(WatchRiskRegistersRequest request, StreamObserver<WatchRiskRegistersResponse> observer) {
        try {
            var snapshot = application.watchRiskRegisters(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Takes a stream of risk registers and answers once at the end.
     */
    @Override
    public StreamObserver<BatchCreateRiskRegistersRequest> batchCreateRiskRegisters(StreamObserver<BatchCreateRiskRegistersResponse> observer) {
        return new StreamObserver<BatchCreateRiskRegistersRequest>() {
            private boolean finished;
            private final BatchCreateRiskRegistersResponse.Builder summary = BatchCreateRiskRegistersResponse.newBuilder();

            @Override
            public void onNext(BatchCreateRiskRegistersRequest request) {
                if (finished) { return; }
                try {
                    var response = application.batchCreateRiskRegisters(request);
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
     * Creates a risk register.
     */
    @Override
    public void cloneRiskRegister(CloneRiskRegisterRequest request, StreamObserver<CloneRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.cloneRiskRegister(request));
    }
}
