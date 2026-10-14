package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.CheckControlEligibilityRequest;
import com.acme.compliance.v1.CheckControlEligibilityResponse;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.GetControlRulesRequest;
import com.acme.compliance.v1.GetControlRulesResponse;
import com.acme.compliance.v1.LintControlsRequest;
import com.acme.compliance.v1.LintControlsResponse;
import com.acme.compliance.v1.NormalizeControlRequest;
import com.acme.compliance.v1.NormalizeControlResponse;
import com.acme.compliance.v1.PreviewControlRequest;
import com.acme.compliance.v1.PreviewControlResponse;
import com.acme.compliance.v1.ScoreControlRequest;
import com.acme.compliance.v1.ScoreControlResponse;
import com.acme.compliance.v1.ValidateControlRequest;
import com.acme.compliance.v1.ValidateControlResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks controls against the rules before anything acts on them.
 */
@GrpcService
public class ControlValidationEndpoint extends ControlValidationServiceGrpc.ControlValidationServiceImplBase {
    protected final ControlValidationApplication application;

    public ControlValidationEndpoint(ControlValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a control.
     */
    @Override
    public void validateControl(ValidateControlRequest request, StreamObserver<ValidateControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateControl(request));
    }

    /**
     * Previews a control.
     */
    @Override
    public void previewControl(PreviewControlRequest request, StreamObserver<PreviewControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewControl(request));
    }

    /**
     * Checks a control.
     */
    @Override
    public void checkControlEligibility(CheckControlEligibilityRequest request, StreamObserver<CheckControlEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkControlEligibility(request));
    }

    /**
     * Normalizes a control.
     */
    @Override
    public void normalizeControl(NormalizeControlRequest request, StreamObserver<NormalizeControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeControl(request));
    }

    /**
     * Scores a control.
     */
    @Override
    public void scoreControl(ScoreControlRequest request, StreamObserver<ScoreControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreControl(request));
    }

    /**
     * Lints a control.
     */
    @Override
    public void lintControls(LintControlsRequest request, StreamObserver<LintControlsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintControls(request));
    }

    /**
     * Returns a control.
     */
    @Override
    public void getControlRules(GetControlRulesRequest request, StreamObserver<GetControlRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getControlRules(request));
    }
}
