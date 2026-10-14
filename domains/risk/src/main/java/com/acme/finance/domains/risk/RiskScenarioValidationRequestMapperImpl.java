package com.acme.finance.domains.risk;

import com.acme.audiences.v1.AudienceServiceGrpc;
import com.acme.audiences.v1.SearchAudiencesRequest;
import com.acme.audiences.v1.SearchAudiencesResponse;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CloneAccessPolicyRequest;
import com.acme.authorization.v1.CloneAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.PreviewPolicyRequest;
import com.acme.insurance.v1.PreviewPolicyResponse;
import com.acme.insurance.v1.ScoreUnderwritingRequest;
import com.acme.insurance.v1.ScoreUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.labeling.v1.LabelingTaskQueryServiceGrpc;
import com.acme.labeling.v1.ResolveLabelingTasksRequest;
import com.acme.labeling.v1.ResolveLabelingTasksResponse;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.CheckRiskScenarioEligibilityRequest;
import com.acme.risk.v1.CheckRiskScenarioEligibilityResponse;
import com.acme.risk.v1.ExplainRiskRegisterDecisionRequest;
import com.acme.risk.v1.ExplainRiskRegisterDecisionResponse;
import com.acme.risk.v1.ExplainRiskScenarioDecisionRequest;
import com.acme.risk.v1.ExplainRiskScenarioDecisionResponse;
import com.acme.risk.v1.GetRiskScenarioRulesRequest;
import com.acme.risk.v1.GetRiskScenarioRulesResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.LintRiskScenariosRequest;
import com.acme.risk.v1.LintRiskScenariosResponse;
import com.acme.risk.v1.NormalizeRiskRegisterRequest;
import com.acme.risk.v1.NormalizeRiskRegisterResponse;
import com.acme.risk.v1.NormalizeRiskScenarioRequest;
import com.acme.risk.v1.NormalizeRiskScenarioResponse;
import com.acme.risk.v1.PreviewRiskRegisterRequest;
import com.acme.risk.v1.PreviewRiskRegisterResponse;
import com.acme.risk.v1.PreviewRiskScenarioRequest;
import com.acme.risk.v1.PreviewRiskScenarioResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.ScoreRiskScenarioRequest;
import com.acme.risk.v1.ScoreRiskScenarioResponse;
import com.acme.risk.v1.ValidateRiskRegisterRequest;
import com.acme.risk.v1.ValidateRiskRegisterResponse;
import com.acme.risk.v1.ValidateRiskScenarioRequest;
import com.acme.risk.v1.ValidateRiskScenarioResponse;
import com.acme.venues.v1.ScoreVenueRequest;
import com.acme.venues.v1.ScoreVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class RiskScenarioValidationRequestMapperImpl implements RiskScenarioValidationRequestMapper {
    @Override
    public CloneAccessPolicyRequest normalizeRiskScenarioStage0(NormalizeRiskScenarioRequest request) {
        return CloneAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRiskScenario().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getRiskScenario().getCustomer().getDisplayName())
                                .setEmail(request.getRiskScenario().getCustomer().getEmail())
                                .setUserId(request.getRiskScenario().getAssignee().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
    }
}
