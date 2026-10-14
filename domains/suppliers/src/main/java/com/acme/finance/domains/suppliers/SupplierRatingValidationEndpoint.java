package com.acme.finance.domains.suppliers;

import com.acme.finance.platform.RpcSupport;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityResponse;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionResponse;
import com.acme.suppliers.v1.GetSupplierRatingRulesRequest;
import com.acme.suppliers.v1.GetSupplierRatingRulesResponse;
import com.acme.suppliers.v1.LintSupplierRatingsRequest;
import com.acme.suppliers.v1.LintSupplierRatingsResponse;
import com.acme.suppliers.v1.NormalizeSupplierRatingRequest;
import com.acme.suppliers.v1.NormalizeSupplierRatingResponse;
import com.acme.suppliers.v1.PreviewSupplierRatingRequest;
import com.acme.suppliers.v1.PreviewSupplierRatingResponse;
import com.acme.suppliers.v1.ScoreSupplierRatingRequest;
import com.acme.suppliers.v1.ScoreSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks supplier ratings against the rules before anything acts on them.
 */
@GrpcService
public class SupplierRatingValidationEndpoint extends SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceImplBase {
    protected final SupplierRatingValidationApplication application;

    public SupplierRatingValidationEndpoint(SupplierRatingValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a supplier rating.
     */
    @Override
    public void validateSupplierRating(ValidateSupplierRatingRequest request, StreamObserver<ValidateSupplierRatingResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateSupplierRating(request));
    }

    /**
     * Previews a supplier rating.
     */
    @Override
    public void previewSupplierRating(PreviewSupplierRatingRequest request, StreamObserver<PreviewSupplierRatingResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewSupplierRating(request));
    }

    /**
     * Checks a supplier rating.
     */
    @Override
    public void checkSupplierRatingEligibility(CheckSupplierRatingEligibilityRequest request, StreamObserver<CheckSupplierRatingEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkSupplierRatingEligibility(request));
    }

    /**
     * Normalizes a supplier rating.
     */
    @Override
    public void normalizeSupplierRating(NormalizeSupplierRatingRequest request, StreamObserver<NormalizeSupplierRatingResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeSupplierRating(request));
    }

    /**
     * Explains a supplier rating.
     */
    @Override
    public void explainSupplierRatingDecision(ExplainSupplierRatingDecisionRequest request, StreamObserver<ExplainSupplierRatingDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainSupplierRatingDecision(request));
    }

    /**
     * Scores a supplier rating.
     */
    @Override
    public void scoreSupplierRating(ScoreSupplierRatingRequest request, StreamObserver<ScoreSupplierRatingResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreSupplierRating(request));
    }

    /**
     * Lints a supplier rating.
     */
    @Override
    public void lintSupplierRatings(LintSupplierRatingsRequest request, StreamObserver<LintSupplierRatingsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintSupplierRatings(request));
    }

    /**
     * Returns a supplier rating.
     */
    @Override
    public void getSupplierRatingRules(GetSupplierRatingRulesRequest request, StreamObserver<GetSupplierRatingRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getSupplierRatingRules(request));
    }
}
