package com.acme.finance.domains.contracts;

import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.DisposalSla;
import com.acme.assets.v1.EstimateDisposalRequest;
import com.acme.assets.v1.EstimateDisposalResponse;
import com.acme.assets.v1.RecalculateDepreciationsRequest;
import com.acme.assets.v1.RecalculateDepreciationsResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.CalculateObligationRequest;
import com.acme.contracts.v1.CalculateObligationResponse;
import com.acme.contracts.v1.CheckObligationEligibilityRequest;
import com.acme.contracts.v1.CheckObligationEligibilityResponse;
import com.acme.contracts.v1.CompareObligationOptionsRequest;
import com.acme.contracts.v1.CompareObligationOptionsResponse;
import com.acme.contracts.v1.ExplainObligationCalculationRequest;
import com.acme.contracts.v1.ExplainObligationCalculationResponse;
import com.acme.contracts.v1.ExplainObligationDecisionRequest;
import com.acme.contracts.v1.ExplainObligationDecisionResponse;
import com.acme.contracts.v1.GetObligationRatesRequest;
import com.acme.contracts.v1.GetObligationRatesResponse;
import com.acme.contracts.v1.GetObligationRulesRequest;
import com.acme.contracts.v1.GetObligationRulesResponse;
import com.acme.contracts.v1.LintObligationsRequest;
import com.acme.contracts.v1.LintObligationsResponse;
import com.acme.contracts.v1.NormalizeObligationRequest;
import com.acme.contracts.v1.NormalizeObligationResponse;
import com.acme.contracts.v1.NormalizeRenewalRequest;
import com.acme.contracts.v1.NormalizeRenewalResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationComment;
import com.acme.contracts.v1.ObligationSla;
import com.acme.contracts.v1.ObligationStatusChange;
import com.acme.contracts.v1.PreviewObligationRequest;
import com.acme.contracts.v1.PreviewObligationResponse;
import com.acme.contracts.v1.QuoteObligationRequest;
import com.acme.contracts.v1.QuoteObligationResponse;
import com.acme.contracts.v1.RecalculateObligationsRequest;
import com.acme.contracts.v1.RecalculateObligationsResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ScoreObligationRequest;
import com.acme.contracts.v1.ScoreObligationResponse;
import com.acme.contracts.v1.ValidateObligationRequest;
import com.acme.contracts.v1.ValidateObligationResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.featureflags.v1.SummarizeFeatureFlagsRequest;
import com.acme.featureflags.v1.SummarizeFeatureFlagsResponse;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.QuoteCollateralRequest;
import com.acme.lending.v1.QuoteCollateralResponse;
import com.acme.onboarding.v1.EquipmentRef;
import com.acme.onboarding.v1.OnboardingPlan;
import com.acme.onboarding.v1.OnboardingPlanCalculationServiceGrpc;
import com.acme.onboarding.v1.SimulateOnboardingPlanRequest;
import com.acme.onboarding.v1.SimulateOnboardingPlanResponse;
import com.acme.risk.v1.ExplainStressTestDecisionRequest;
import com.acme.risk.v1.ExplainStressTestDecisionResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCardSettlement;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.acme.risk.v1.ValidateStressTestRequest;
import com.acme.risk.v1.ValidateStressTestResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class ObligationValidationRequestMapperImpl implements ObligationValidationRequestMapper {
    @Override
    public SummarizeFeatureFlagsRequest checkObligationEligibilityStage0(CheckObligationEligibilityRequest request) {
        return SummarizeFeatureFlagsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeFeatureFlagsRequest.Details.newBuilder()
                        .putAllFilters(request.getObligation().getCustomFieldsMap())
                        .build())
                .setReason(request.getReason())
                .build();
    }

    @Override
    public SimulateOnboardingPlanRequest normalizeObligationStage0(NormalizeObligationRequest request) {
        return SimulateOnboardingPlanRequest.newBuilder()
                .setContext(request.getContext())
                .setOnboardingPlan(OnboardingPlan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getObligation().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getObligation().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(EquipmentRef.newBuilder()
                                .setDisplayName(request.getObligation().getAssignee().getDisplayName())
                                .build())
                        .setDueAt(request.getObligation().getSla().getDueAt())
                        .build())
                .build();
    }
}
