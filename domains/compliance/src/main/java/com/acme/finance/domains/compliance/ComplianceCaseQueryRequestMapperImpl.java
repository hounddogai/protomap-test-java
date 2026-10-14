package com.acme.finance.domains.compliance;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.BrowseComplianceCasesRequest;
import com.acme.compliance.v1.BrowseComplianceCasesResponse;
import com.acme.compliance.v1.CalculateSanctionRequest;
import com.acme.compliance.v1.CalculateSanctionResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlBlockAction;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.CountComplianceCasesRequest;
import com.acme.compliance.v1.CountComplianceCasesResponse;
import com.acme.compliance.v1.ExportComplianceCasesRequest;
import com.acme.compliance.v1.ExportComplianceCasesResponse;
import com.acme.compliance.v1.FindComplianceCasesRequest;
import com.acme.compliance.v1.FindComplianceCasesResponse;
import com.acme.compliance.v1.LookupComplianceCaseRequest;
import com.acme.compliance.v1.LookupComplianceCaseResponse;
import com.acme.compliance.v1.PeekComplianceCaseRequest;
import com.acme.compliance.v1.PeekComplianceCaseResponse;
import com.acme.compliance.v1.PreviewComplianceCaseRequest;
import com.acme.compliance.v1.PreviewComplianceCaseResponse;
import com.acme.compliance.v1.PreviewControlRequest;
import com.acme.compliance.v1.PreviewControlResponse;
import com.acme.compliance.v1.RecalculateControlsRequest;
import com.acme.compliance.v1.RecalculateControlsResponse;
import com.acme.compliance.v1.ResolveComplianceCasesRequest;
import com.acme.compliance.v1.ResolveComplianceCasesResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.SuggestComplianceCasesRequest;
import com.acme.compliance.v1.SuggestComplianceCasesResponse;
import com.acme.compliance.v1.SummarizeComplianceCasesRequest;
import com.acme.compliance.v1.SummarizeComplianceCasesResponse;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.risk.v1.CheckRiskScenarioEligibilityRequest;
import com.acme.risk.v1.CheckRiskScenarioEligibilityResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.shows.v1.ResolveShowsRequest;
import com.acme.shows.v1.ResolveShowsResponse;
import com.acme.shows.v1.ShowQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class ComplianceCaseQueryRequestMapperImpl implements ComplianceCaseQueryRequestMapper {
    @Override
    public ResolveShowsRequest findComplianceCasesStage0(FindComplianceCasesRequest request) {
        return ResolveShowsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
    }

    @Override
    public CheckRiskScenarioEligibilityRequest exportComplianceCasesStage0(ExportComplianceCasesRequest request) {
        return CheckRiskScenarioEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckRiskScenarioEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
    }
}
