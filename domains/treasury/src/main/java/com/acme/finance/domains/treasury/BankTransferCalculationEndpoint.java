package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.CalculateBankTransferRequest;
import com.acme.treasury.v1.CalculateBankTransferResponse;
import com.acme.treasury.v1.CompareBankTransferOptionsRequest;
import com.acme.treasury.v1.CompareBankTransferOptionsResponse;
import com.acme.treasury.v1.EstimateBankTransferRequest;
import com.acme.treasury.v1.EstimateBankTransferResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.GetBankTransferRatesRequest;
import com.acme.treasury.v1.GetBankTransferRatesResponse;
import com.acme.treasury.v1.RecalculateBankTransfersRequest;
import com.acme.treasury.v1.RecalculateBankTransfersResponse;
import com.acme.treasury.v1.SimulateBankTransferRequest;
import com.acme.treasury.v1.SimulateBankTransferResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a bank transfer, such as totals, taxes, and estimates.
 */
@GrpcService
public class BankTransferCalculationEndpoint extends BankTransferCalculationServiceGrpc.BankTransferCalculationServiceImplBase {
    protected final BankTransferCalculationApplication application;

    public BankTransferCalculationEndpoint(BankTransferCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a bank transfer.
     */
    @Override
    public void calculateBankTransfer(CalculateBankTransferRequest request, StreamObserver<CalculateBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateBankTransfer(request));
    }

    /**
     * Estimates a bank transfer.
     */
    @Override
    public void estimateBankTransfer(EstimateBankTransferRequest request, StreamObserver<EstimateBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateBankTransfer(request));
    }

    /**
     * Compares a bank transfer.
     */
    @Override
    public void compareBankTransferOptions(CompareBankTransferOptionsRequest request, StreamObserver<CompareBankTransferOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareBankTransferOptions(request));
    }

    /**
     * Recalculates a bank transfer.
     */
    @Override
    public void recalculateBankTransfers(RecalculateBankTransfersRequest request, StreamObserver<RecalculateBankTransfersResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateBankTransfers(request));
    }

    /**
     * Simulates a bank transfer.
     */
    @Override
    public void simulateBankTransfer(SimulateBankTransferRequest request, StreamObserver<SimulateBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateBankTransfer(request));
    }

    /**
     * Explains a bank transfer.
     */
    @Override
    public void explainBankTransferCalculation(ExplainBankTransferCalculationRequest request, StreamObserver<ExplainBankTransferCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainBankTransferCalculation(request));
    }

    /**
     * Returns a bank transfer.
     */
    @Override
    public void getBankTransferRates(GetBankTransferRatesRequest request, StreamObserver<GetBankTransferRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getBankTransferRates(request));
    }
}
