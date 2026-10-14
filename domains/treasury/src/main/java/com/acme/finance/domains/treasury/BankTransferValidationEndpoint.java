package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.CheckBankTransferEligibilityRequest;
import com.acme.treasury.v1.CheckBankTransferEligibilityResponse;
import com.acme.treasury.v1.ExplainBankTransferDecisionRequest;
import com.acme.treasury.v1.ExplainBankTransferDecisionResponse;
import com.acme.treasury.v1.NormalizeBankTransferRequest;
import com.acme.treasury.v1.NormalizeBankTransferResponse;
import com.acme.treasury.v1.PreviewBankTransferRequest;
import com.acme.treasury.v1.PreviewBankTransferResponse;
import com.acme.treasury.v1.ScoreBankTransferRequest;
import com.acme.treasury.v1.ScoreBankTransferResponse;
import com.acme.treasury.v1.ValidateBankTransferRequest;
import com.acme.treasury.v1.ValidateBankTransferResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks bank transfers against the rules before anything acts on them.
 */
@GrpcService
public class BankTransferValidationEndpoint extends BankTransferValidationServiceGrpc.BankTransferValidationServiceImplBase {
    protected final BankTransferValidationApplication application;

    public BankTransferValidationEndpoint(BankTransferValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a bank transfer.
     */
    @Override
    public void validateBankTransfer(ValidateBankTransferRequest request, StreamObserver<ValidateBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateBankTransfer(request));
    }

    /**
     * Previews a bank transfer.
     */
    @Override
    public void previewBankTransfer(PreviewBankTransferRequest request, StreamObserver<PreviewBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewBankTransfer(request));
    }

    /**
     * Checks a bank transfer.
     */
    @Override
    public void checkBankTransferEligibility(CheckBankTransferEligibilityRequest request, StreamObserver<CheckBankTransferEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkBankTransferEligibility(request));
    }

    /**
     * Normalizes a bank transfer.
     */
    @Override
    public void normalizeBankTransfer(NormalizeBankTransferRequest request, StreamObserver<NormalizeBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeBankTransfer(request));
    }

    /**
     * Explains a bank transfer.
     */
    @Override
    public void explainBankTransferDecision(ExplainBankTransferDecisionRequest request, StreamObserver<ExplainBankTransferDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainBankTransferDecision(request));
    }

    /**
     * Scores a bank transfer.
     */
    @Override
    public void scoreBankTransfer(ScoreBankTransferRequest request, StreamObserver<ScoreBankTransferResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreBankTransfer(request));
    }
}
