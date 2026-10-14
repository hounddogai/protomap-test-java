package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.ComparePolicyOptionsRequest;
import com.acme.insurance.v1.ComparePolicyOptionsResponse;
import com.acme.insurance.v1.EstimatePolicyRequest;
import com.acme.insurance.v1.EstimatePolicyResponse;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.GetPolicyRatesRequest;
import com.acme.insurance.v1.GetPolicyRatesResponse;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.insurance.v1.RecalculatePoliciesRequest;
import com.acme.insurance.v1.RecalculatePoliciesResponse;
import com.acme.insurance.v1.SimulatePolicyRequest;
import com.acme.insurance.v1.SimulatePolicyResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Computes the figures of a policy, such as totals, taxes, and estimates.
 */
@GrpcService
public class PolicyCalculationEndpoint extends PolicyCalculationServiceGrpc.PolicyCalculationServiceImplBase {
    protected final PolicyCalculationApplication application;

    public PolicyCalculationEndpoint(PolicyCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a policy.
     */
    @Override
    public void calculatePolicy(CalculatePolicyRequest request, StreamObserver<CalculatePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculatePolicy(request));
    }

    /**
     * Estimates a policy.
     */
    @Override
    public void estimatePolicy(EstimatePolicyRequest request, StreamObserver<EstimatePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimatePolicy(request));
    }

    /**
     * Quotes a policy.
     */
    @Override
    public void quotePolicy(QuotePolicyRequest request, StreamObserver<QuotePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.quotePolicy(request));
    }

    /**
     * Compares a policy.
     */
    @Override
    public void comparePolicyOptions(ComparePolicyOptionsRequest request, StreamObserver<ComparePolicyOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.comparePolicyOptions(request));
    }

    /**
     * Recalculates a policy.
     */
    @Override
    public void recalculatePolicies(RecalculatePoliciesRequest request, StreamObserver<RecalculatePoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculatePolicies(request));
    }

    /**
     * Simulates a policy.
     */
    @Override
    public void simulatePolicy(SimulatePolicyRequest request, StreamObserver<SimulatePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulatePolicy(request));
    }

    /**
     * Explains a policy.
     */
    @Override
    public void explainPolicyCalculation(ExplainPolicyCalculationRequest request, StreamObserver<ExplainPolicyCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainPolicyCalculation(request));
    }

    /**
     * Returns a policy.
     */
    @Override
    public void getPolicyRates(GetPolicyRatesRequest request, StreamObserver<GetPolicyRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPolicyRates(request));
    }
}
