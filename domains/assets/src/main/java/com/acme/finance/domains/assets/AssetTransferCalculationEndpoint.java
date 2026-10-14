package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.CalculateAssetTransferRequest;
import com.acme.assets.v1.CalculateAssetTransferResponse;
import com.acme.assets.v1.CompareAssetTransferOptionsRequest;
import com.acme.assets.v1.CompareAssetTransferOptionsResponse;
import com.acme.assets.v1.EstimateAssetTransferRequest;
import com.acme.assets.v1.EstimateAssetTransferResponse;
import com.acme.assets.v1.ExplainAssetTransferCalculationRequest;
import com.acme.assets.v1.ExplainAssetTransferCalculationResponse;
import com.acme.assets.v1.GetAssetTransferRatesRequest;
import com.acme.assets.v1.GetAssetTransferRatesResponse;
import com.acme.assets.v1.QuoteAssetTransferRequest;
import com.acme.assets.v1.QuoteAssetTransferResponse;
import com.acme.assets.v1.RecalculateAssetTransfersRequest;
import com.acme.assets.v1.RecalculateAssetTransfersResponse;
import com.acme.assets.v1.SimulateAssetTransferRequest;
import com.acme.assets.v1.SimulateAssetTransferResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of an asset transfer, such as totals, taxes, and estimates.
 */
@GrpcService
public class AssetTransferCalculationEndpoint extends AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceImplBase {
    protected final AssetTransferCalculationApplication application;

    public AssetTransferCalculationEndpoint(AssetTransferCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates an asset transfer.
     */
    @Override
    public void calculateAssetTransfer(CalculateAssetTransferRequest request, StreamObserver<CalculateAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateAssetTransfer(request));
    }

    /**
     * Estimates an asset transfer.
     */
    @Override
    public void estimateAssetTransfer(EstimateAssetTransferRequest request, StreamObserver<EstimateAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateAssetTransfer(request));
    }

    /**
     * Quotes an asset transfer.
     */
    @Override
    public void quoteAssetTransfer(QuoteAssetTransferRequest request, StreamObserver<QuoteAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteAssetTransfer(request));
    }

    /**
     * Compares an asset transfer.
     */
    @Override
    public void compareAssetTransferOptions(CompareAssetTransferOptionsRequest request, StreamObserver<CompareAssetTransferOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareAssetTransferOptions(request));
    }

    /**
     * Recalculates an asset transfer.
     */
    @Override
    public void recalculateAssetTransfers(RecalculateAssetTransfersRequest request, StreamObserver<RecalculateAssetTransfersResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateAssetTransfers(request));
    }

    /**
     * Simulates an asset transfer.
     */
    @Override
    public void simulateAssetTransfer(SimulateAssetTransferRequest request, StreamObserver<SimulateAssetTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateAssetTransfer(request));
    }

    /**
     * Explains an asset transfer.
     */
    @Override
    public void explainAssetTransferCalculation(ExplainAssetTransferCalculationRequest request, StreamObserver<ExplainAssetTransferCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainAssetTransferCalculation(request));
    }

    /**
     * Returns an asset transfer.
     */
    @Override
    public void getAssetTransferRates(GetAssetTransferRatesRequest request, StreamObserver<GetAssetTransferRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAssetTransferRates(request));
    }
}
