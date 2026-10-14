package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.CheckMileageClaimEligibilityRequest;
import com.acme.expenses.v1.CheckMileageClaimEligibilityResponse;
import com.acme.expenses.v1.GetMileageClaimRulesRequest;
import com.acme.expenses.v1.GetMileageClaimRulesResponse;
import com.acme.expenses.v1.LintMileageClaimsRequest;
import com.acme.expenses.v1.LintMileageClaimsResponse;
import com.acme.expenses.v1.MileageClaimValidationServiceGrpc;
import com.acme.expenses.v1.NormalizeMileageClaimRequest;
import com.acme.expenses.v1.NormalizeMileageClaimResponse;
import com.acme.expenses.v1.PreviewMileageClaimRequest;
import com.acme.expenses.v1.PreviewMileageClaimResponse;
import com.acme.expenses.v1.ScoreMileageClaimRequest;
import com.acme.expenses.v1.ScoreMileageClaimResponse;
import com.acme.expenses.v1.ValidateMileageClaimRequest;
import com.acme.expenses.v1.ValidateMileageClaimResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Checks mileage claims against the rules before anything acts on them.
 */
public abstract class AbstractMileageClaimValidationEndpoint extends MileageClaimValidationServiceGrpc.MileageClaimValidationServiceImplBase {
    protected final MileageClaimValidationApplication application;

    protected AbstractMileageClaimValidationEndpoint(MileageClaimValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a mileage claim.
     */
    @Override
    public void validateMileageClaim(ValidateMileageClaimRequest request, StreamObserver<ValidateMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateMileageClaim(request));
    }

    /**
     * Previews a mileage claim.
     */
    @Override
    public void previewMileageClaim(PreviewMileageClaimRequest request, StreamObserver<PreviewMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewMileageClaim(request));
    }

    /**
     * Checks a mileage claim.
     */
    @Override
    public void checkMileageClaimEligibility(CheckMileageClaimEligibilityRequest request, StreamObserver<CheckMileageClaimEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkMileageClaimEligibility(request));
    }

    /**
     * Normalizes a mileage claim.
     */
    @Override
    public void normalizeMileageClaim(NormalizeMileageClaimRequest request, StreamObserver<NormalizeMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeMileageClaim(request));
    }

    /**
     * Scores a mileage claim.
     */
    @Override
    public void scoreMileageClaim(ScoreMileageClaimRequest request, StreamObserver<ScoreMileageClaimResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreMileageClaim(request));
    }

    /**
     * Lints a mileage claim.
     */
    @Override
    public void lintMileageClaims(LintMileageClaimsRequest request, StreamObserver<LintMileageClaimsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintMileageClaims(request));
    }

    /**
     * Returns a mileage claim.
     */
    @Override
    public void getMileageClaimRules(GetMileageClaimRulesRequest request, StreamObserver<GetMileageClaimRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getMileageClaimRules(request));
    }
}
