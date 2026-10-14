package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.CalculateAttestationRequest;
import com.acme.compliance.v1.CalculateAttestationResponse;
import com.acme.compliance.v1.CompareAttestationOptionsRequest;
import com.acme.compliance.v1.CompareAttestationOptionsResponse;
import com.acme.compliance.v1.EstimateAttestationRequest;
import com.acme.compliance.v1.EstimateAttestationResponse;
import com.acme.compliance.v1.ExplainAttestationCalculationRequest;
import com.acme.compliance.v1.ExplainAttestationCalculationResponse;
import com.acme.compliance.v1.GetAttestationRatesRequest;
import com.acme.compliance.v1.GetAttestationRatesResponse;
import com.acme.compliance.v1.QuoteAttestationRequest;
import com.acme.compliance.v1.QuoteAttestationResponse;
import com.acme.compliance.v1.RecalculateAttestationsRequest;
import com.acme.compliance.v1.RecalculateAttestationsResponse;
import com.acme.compliance.v1.SimulateAttestationRequest;
import com.acme.compliance.v1.SimulateAttestationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of an attestation, such as totals, taxes, and estimates.
 */
@GrpcService
public class AttestationCalculationEndpoint extends AttestationCalculationServiceGrpc.AttestationCalculationServiceImplBase {
    protected final AttestationCalculationApplication application;

    public AttestationCalculationEndpoint(AttestationCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates an attestation.
     */
    @Override
    public void calculateAttestation(CalculateAttestationRequest request, StreamObserver<CalculateAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateAttestation(request));
    }

    /**
     * Estimates an attestation.
     */
    @Override
    public void estimateAttestation(EstimateAttestationRequest request, StreamObserver<EstimateAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateAttestation(request));
    }

    /**
     * Quotes an attestation.
     */
    @Override
    public void quoteAttestation(QuoteAttestationRequest request, StreamObserver<QuoteAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteAttestation(request));
    }

    /**
     * Compares an attestation.
     */
    @Override
    public void compareAttestationOptions(CompareAttestationOptionsRequest request, StreamObserver<CompareAttestationOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareAttestationOptions(request));
    }

    /**
     * Recalculates an attestation.
     */
    @Override
    public void recalculateAttestations(RecalculateAttestationsRequest request, StreamObserver<RecalculateAttestationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateAttestations(request));
    }

    /**
     * Simulates an attestation.
     */
    @Override
    public void simulateAttestation(SimulateAttestationRequest request, StreamObserver<SimulateAttestationResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateAttestation(request));
    }

    /**
     * Explains an attestation.
     */
    @Override
    public void explainAttestationCalculation(ExplainAttestationCalculationRequest request, StreamObserver<ExplainAttestationCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainAttestationCalculation(request));
    }

    /**
     * Returns an attestation.
     */
    @Override
    public void getAttestationRates(GetAttestationRatesRequest request, StreamObserver<GetAttestationRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAttestationRates(request));
    }
}
