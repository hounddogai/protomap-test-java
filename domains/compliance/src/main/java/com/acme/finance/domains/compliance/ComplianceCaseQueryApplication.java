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
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about compliance cases, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ComplianceCaseQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(ComplianceCaseQueryApplication.class);

    private final ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking;

    private final SanctionCalculationServiceGrpc.SanctionCalculationServiceBlockingStub complianceSanctionCalculationServiceBlocking;

    private final ComplianceCaseQueryRequestMapper mapper;

    private final ComplianceRepository repository;

    private final RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking;

    private final ShowQueryServiceGrpc.ShowQueryServiceBlockingStub showsShowQueryServiceBlocking;

    public ComplianceCaseQueryApplication(
            ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking,
            SanctionCalculationServiceGrpc.SanctionCalculationServiceBlockingStub complianceSanctionCalculationServiceBlocking,
            ComplianceCaseQueryRequestMapper mapper,
            ComplianceRepository repository,
            RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking,
            ShowQueryServiceGrpc.ShowQueryServiceBlockingStub showsShowQueryServiceBlocking) {
        this.complianceComplianceCaseValidationServiceBlocking = complianceComplianceCaseValidationServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.complianceControlValidationServiceBlocking = complianceControlValidationServiceBlocking;
        this.complianceSanctionCalculationServiceBlocking = complianceSanctionCalculationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskScenarioValidationServiceBlocking = riskRiskScenarioValidationServiceBlocking;
        this.showsShowQueryServiceBlocking = showsShowQueryServiceBlocking;
    }

    /**
     * Returns a compliance case.
     */
    public LookupComplianceCaseResponse lookupComplianceCase(LookupComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getComplianceCaseId(), "LookupComplianceCase");

        var outbound0 = CalculateSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getContext().getActor().getEmail())
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .setParentId(request.getComplianceCaseId())
                                        .build())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            CalculateSanctionResponse fetch(CalculateSanctionRequest input) {
                return complianceSanctionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateSanction(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        return LookupComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists compliance cases a page at a time.
     */
    public BrowseComplianceCasesResponse browseComplianceCases(BrowseComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getContext().getTraceId(), "BrowseComplianceCases");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseComplianceCases", request.getPage().getPageToken());
        return BrowseComplianceCasesResponse.getDefaultInstance();
    }

    /**
     * Lists compliance cases a page at a time.
     */
    public FindComplianceCasesResponse findComplianceCases(FindComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getContext().getTraceId(), "FindComplianceCases");

        var outbound0 = mapper.findComplianceCasesStage0(request);
        var reply0 = showsShowQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveShows(outbound0);

        var outbound1 = RecalculateControlsRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateControls(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindComplianceCases", request.getPage().getPageToken());
        return FindComplianceCasesResponse.newBuilder()
                .addComplianceCases(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getControl().getOwner().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getControl().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Counts compliance cases by status.
     */
    public CountComplianceCasesResponse countComplianceCases(CountComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getContext().getTraceId(), "CountComplianceCases");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountComplianceCases", request.getPage().getPageToken());
        return CountComplianceCasesResponse.getDefaultInstance();
    }

    /**
     * Returns several compliance cases at once.
     */
    public ResolveComplianceCasesResponse resolveComplianceCases(ResolveComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getContext().getTraceId(), "ResolveComplianceCases");
        return ResolveComplianceCasesResponse.getDefaultInstance();
    }

    /**
     * Streams every matching compliance case.
     */
    public ExportComplianceCasesResponse exportComplianceCases(ExportComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getContext().getTraceId(), "ExportComplianceCases");

        var outbound0 = mapper.exportComplianceCasesStage0(request);
        var reply0 = riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRiskScenarioEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportComplianceCases", request.getFilter().getQuery());
        return ExportComplianceCasesResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getRiskScenario().getCustomer().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Summarizes a compliance case.
     */
    public SummarizeComplianceCasesResponse summarizeComplianceCases(SummarizeComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getComplianceCaseId(), "SummarizeComplianceCases");

        var outbound0 = PreviewControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setBlock(ControlBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .build())
                .build();
        Function<PreviewControlRequest, PreviewControlResponse> operation0 = complianceControlValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewControl;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeComplianceCases", request.getComplianceCaseId());
        return SummarizeComplianceCasesResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists compliance cases a page at a time.
     */
    public SuggestComplianceCasesResponse suggestComplianceCases(SuggestComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getContext().getTraceId(), "SuggestComplianceCases");

        var outbound0 = PreviewComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewComplianceCaseRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var adapter0 = new Function<PreviewComplianceCaseRequest, PreviewComplianceCaseResponse>() {
            @Override
            public PreviewComplianceCaseResponse apply(PreviewComplianceCaseRequest input) {
                return complianceComplianceCaseValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewComplianceCase(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestComplianceCases", request.getPage().getPageToken());
        return SuggestComplianceCasesResponse.newBuilder()
                .addComplianceCases(ComplianceCase.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getComplianceCase().getRequester().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns a compliance case.
     */
    public PeekComplianceCaseResponse peekComplianceCase(PeekComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getComplianceCaseId(), "PeekComplianceCase");
        return PeekComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .build())
                .build();
    }
}
