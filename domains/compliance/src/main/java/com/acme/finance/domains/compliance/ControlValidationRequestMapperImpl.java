package com.acme.finance.domains.compliance;

import com.acme.careplans.v1.CarePlan;
import com.acme.careplans.v1.CarePlanCalculationServiceGrpc;
import com.acme.careplans.v1.CarePlanLine;
import com.acme.careplans.v1.CarePlanLineTax;
import com.acme.careplans.v1.QuoteCarePlanRequest;
import com.acme.careplans.v1.QuoteCarePlanResponse;
import com.acme.collections.v1.ValidateWriteOffRequest;
import com.acme.collections.v1.ValidateWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffLine;
import com.acme.collections.v1.WriteOffLineTax;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CheckControlEligibilityRequest;
import com.acme.compliance.v1.CheckControlEligibilityResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlRouteAction;
import com.acme.compliance.v1.ExplainControlCalculationRequest;
import com.acme.compliance.v1.ExplainControlCalculationResponse;
import com.acme.compliance.v1.GetControlRatesRequest;
import com.acme.compliance.v1.GetControlRatesResponse;
import com.acme.compliance.v1.GetControlRulesRequest;
import com.acme.compliance.v1.GetControlRulesResponse;
import com.acme.compliance.v1.LintControlsRequest;
import com.acme.compliance.v1.LintControlsResponse;
import com.acme.compliance.v1.NormalizeControlRequest;
import com.acme.compliance.v1.NormalizeControlResponse;
import com.acme.compliance.v1.PreviewComplianceCaseRequest;
import com.acme.compliance.v1.PreviewComplianceCaseResponse;
import com.acme.compliance.v1.PreviewControlRequest;
import com.acme.compliance.v1.PreviewControlResponse;
import com.acme.compliance.v1.RecalculateComplianceCasesRequest;
import com.acme.compliance.v1.RecalculateComplianceCasesResponse;
import com.acme.compliance.v1.RecalculateControlsRequest;
import com.acme.compliance.v1.RecalculateControlsResponse;
import com.acme.compliance.v1.ScoreControlRequest;
import com.acme.compliance.v1.ScoreControlResponse;
import com.acme.compliance.v1.SimulateAttestationRequest;
import com.acme.compliance.v1.SimulateAttestationResponse;
import com.acme.compliance.v1.SimulateControlRequest;
import com.acme.compliance.v1.SimulateControlResponse;
import com.acme.compliance.v1.ValidateControlRequest;
import com.acme.compliance.v1.ValidateControlResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.identity.v1.ValidateUserRequest;
import com.acme.identity.v1.ValidateUserResponse;
import com.acme.insurance.v1.CheckPremiumEligibilityRequest;
import com.acme.insurance.v1.CheckPremiumEligibilityResponse;
import com.acme.insurance.v1.EstimateUnderwritingRequest;
import com.acme.insurance.v1.EstimateUnderwritingResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.venues.v1.FindVenuesRequest;
import com.acme.venues.v1.FindVenuesResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class ControlValidationRequestMapperImpl implements ControlValidationRequestMapper {
    @Override
    public SimulateControlRequest checkControlEligibilityStage0(CheckControlEligibilityRequest request) {
        return SimulateControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getControl().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getControl().getOwner().getDisplayName())
                                .setUserId(request.getControl().getOwner().getUserId())
                                .build())
                        .build())
                .build();
    }

    @Override
    public ValidateWriteOffRequest normalizeControlStage1(NormalizeControlRequest request, RecalculateControlsResponse reply0) {
        return ValidateWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .addLines(WriteOffLine.newBuilder()
                                .setTax(WriteOffLineTax.newBuilder()
                                        .setAmount(reply0.getControl().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    @Override
    public ValidateUserRequest lintControlsStage1(LintControlsRequest request) {
        return ValidateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getControl().getAudit().getCreatedAt())
                                .setEtag(request.getControl().getAudit().getEtag())
                                .setUpdatedAt(request.getControl().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
    }
}
