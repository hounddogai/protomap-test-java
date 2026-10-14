package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.AdvanceFixedAssetWorkflowRequest;
import com.acme.assets.v1.AdvanceFixedAssetWorkflowResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetWorkflowServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.RecordAuditTrailVersionRequest;
import com.acme.audit.v1.RecordAuditTrailVersionResponse;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.BrowseAccessPoliciesRequest;
import com.acme.authorization.v1.BrowseAccessPoliciesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.budgeting.v1.CountVarianceTotalsRequest;
import com.acme.budgeting.v1.CountVarianceTotalsResponse;
import com.acme.budgeting.v1.DeleteVarianceReportRequest;
import com.acme.budgeting.v1.DeleteVarianceReportResponse;
import com.acme.budgeting.v1.FetchVarianceReportRequest;
import com.acme.budgeting.v1.FetchVarianceReportResponse;
import com.acme.budgeting.v1.GenerateVarianceReportRequest;
import com.acme.budgeting.v1.GenerateVarianceReportResponse;
import com.acme.budgeting.v1.ListVarianceImportsRequest;
import com.acme.budgeting.v1.ListVarianceImportsResponse;
import com.acme.budgeting.v1.ListVarianceReportsRequest;
import com.acme.budgeting.v1.ListVarianceReportsResponse;
import com.acme.budgeting.v1.ResolveVarianceConflictRequest;
import com.acme.budgeting.v1.ResolveVarianceConflictResponse;
import com.acme.budgeting.v1.ScheduleVarianceReportRequest;
import com.acme.budgeting.v1.ScheduleVarianceReportResponse;
import com.acme.budgeting.v1.ShareVarianceReportRequest;
import com.acme.budgeting.v1.ShareVarianceReportResponse;
import com.acme.budgeting.v1.SimulateBudgetLineRequest;
import com.acme.budgeting.v1.SimulateBudgetLineResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.StreamVarianceMetricsRequest;
import com.acme.budgeting.v1.StreamVarianceMetricsResponse;
import com.acme.budgeting.v1.UpdateBudgetRequest;
import com.acme.budgeting.v1.UpdateBudgetResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.budgeting.v1.VarianceSla;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.CancelAttestationWorkflowRequest;
import com.acme.compliance.v1.CancelAttestationWorkflowResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseApprovalServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseChannelOrigin;
import com.acme.compliance.v1.ComplianceCaseWorkflowServiceGrpc;
import com.acme.compliance.v1.EscalateComplianceCaseRequest;
import com.acme.compliance.v1.EscalateComplianceCaseResponse;
import com.acme.compliance.v1.RecallComplianceCaseRequest;
import com.acme.compliance.v1.RecallComplianceCaseResponse;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowResponse;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.ResumeCoverageWorkflowRequest;
import com.acme.insurance.v1.ResumeCoverageWorkflowResponse;
import com.acme.licensing.v1.CompareSoftwareLicenseOptionsRequest;
import com.acme.licensing.v1.CompareSoftwareLicenseOptionsResponse;
import com.acme.licensing.v1.SoftwareLicense;
import com.acme.licensing.v1.SoftwareLicenseBankFunding;
import com.acme.licensing.v1.SoftwareLicenseCalculationServiceGrpc;
import com.acme.suppliers.v1.BatchGetSuppliersRequest;
import com.acme.suppliers.v1.BatchGetSuppliersResponse;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityResponse;
import com.acme.suppliers.v1.SupplierContractRef;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about variances. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class VarianceReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(VarianceReportingApplication.class);

    private final FixedAssetWorkflowServiceGrpc.FixedAssetWorkflowServiceBlockingStub assetsFixedAssetWorkflowServiceBlocking;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceFutureStub auditAuditEventQueryServiceFuture;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking;

    private final VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceBlockingStub budgetingVarianceSyncServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationWorkflowServiceGrpc.AttestationWorkflowServiceBlockingStub complianceAttestationWorkflowServiceBlocking;

    private final ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceBlockingStub complianceComplianceCaseApprovalServiceBlocking;

    private final ComplianceCaseWorkflowServiceGrpc.ComplianceCaseWorkflowServiceBlockingStub complianceComplianceCaseWorkflowServiceBlocking;

    private final ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking;

    private final VarianceReportingRequestMapper mapper;

    private final BudgetingRepository repository;

    private final Executor rpcExecutor;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public VarianceReportingApplication(
            FixedAssetWorkflowServiceGrpc.FixedAssetWorkflowServiceBlockingStub assetsFixedAssetWorkflowServiceBlocking,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceFutureStub auditAuditEventQueryServiceFuture,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking,
            VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking,
            VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync,
            VarianceSyncServiceGrpc.VarianceSyncServiceBlockingStub budgetingVarianceSyncServiceBlocking,
            ChannelRegistry channels,
            AttestationWorkflowServiceGrpc.AttestationWorkflowServiceBlockingStub complianceAttestationWorkflowServiceBlocking,
            ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceBlockingStub complianceComplianceCaseApprovalServiceBlocking,
            ComplianceCaseWorkflowServiceGrpc.ComplianceCaseWorkflowServiceBlockingStub complianceComplianceCaseWorkflowServiceBlocking,
            ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking,
            VarianceReportingRequestMapper mapper,
            BudgetingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.assetsFixedAssetWorkflowServiceBlocking = assetsFixedAssetWorkflowServiceBlocking;
        this.auditAuditEventQueryServiceFuture = auditAuditEventQueryServiceFuture;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.budgetingBudgetLineCalculationServiceBlocking = budgetingBudgetLineCalculationServiceBlocking;
        this.budgetingVarianceImportServiceBlocking = budgetingVarianceImportServiceBlocking;
        this.budgetingVarianceSyncServiceAsync = budgetingVarianceSyncServiceAsync;
        this.budgetingVarianceSyncServiceBlocking = budgetingVarianceSyncServiceBlocking;
        this.channels = channels;
        this.complianceAttestationWorkflowServiceBlocking = complianceAttestationWorkflowServiceBlocking;
        this.complianceComplianceCaseApprovalServiceBlocking = complianceComplianceCaseApprovalServiceBlocking;
        this.complianceComplianceCaseWorkflowServiceBlocking = complianceComplianceCaseWorkflowServiceBlocking;
        this.contractsContractServiceAsync = contractsContractServiceAsync;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.insuranceCoverageWorkflowServiceBlocking = insuranceCoverageWorkflowServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    /**
     * Generates a variance.
     */
    public GenerateVarianceReportResponse generateVarianceReport(GenerateVarianceReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateVarianceReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "GenerateVarianceReport");

        var outbound0 = EscalateComplianceCaseRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(complianceComplianceCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateComplianceCase(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CheckSupplierRiskEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setCustomer(SupplierContractRef.newBuilder()
                                .setNumber(switch (reply0.getComplianceCase().getOriginCase()) {
                                    case CUSTOMER -> reply0.getComplianceCase().getCustomer().getNumber();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkSupplierRiskEligibility(outbound1);

        var outbound2 = BatchCreateContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateContractsRequest.StreamHeader.newBuilder()
                        .setFormat(request.getDetails().getFormat())
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateContractsResponse.getDefaultInstance());
        var input2 = contractsContractServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateContracts(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamVarianceChangesRequest.newBuilder()
                .setChange(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamVarianceChangesResponse.getDefaultInstance());
        var input3 = budgetingVarianceSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamVarianceChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateVarianceReport", request.getVarianceId());
        return GenerateVarianceReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a variance.
     */
    public FetchVarianceReportResponse fetchVarianceReport(FetchVarianceReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchVarianceReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "FetchVarianceReport");

        var outbound0 = ResumeComplianceCaseWorkflowRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = complianceComplianceCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeComplianceCaseWorkflow(outbound0);
        return FetchVarianceReportResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getComplianceCase().getAssignee().getDisplayName())
                                .build())
                        .addAllTags(reply0.getComplianceCase().getTagsList())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Schedules a variance.
     */
    public ScheduleVarianceReportResponse scheduleVarianceReport(ScheduleVarianceReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleVarianceReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "ScheduleVarianceReport");

        var outbound0 = SimulateBudgetLineRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setBudgetLine(BudgetLine.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        Function<SimulateBudgetLineRequest, SimulateBudgetLineResponse> operation0 = budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::simulateBudgetLine;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = ResumeCoverageWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getBudgetLine().getAudit().getCreatedAt())
                                .setEtag(reply0.getBudgetLine().getAudit().getEtag())
                                .setUpdatedAt(reply0.getBudgetLine().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setDetails(ResumeCoverageWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(reply0.getBudgetLine().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = insuranceCoverageWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeCoverageWorkflow(outbound1);

        var outbound2 = BrowseAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseAccessPoliciesRequest.Filter.newBuilder()
                        .setRegion(reply0.getBudgetLine().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseAccessPolicies(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleVarianceReport", request.getVarianceId());
        return ScheduleVarianceReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Streams the changes of variances as they happen.
     */
    public StreamVarianceMetricsResponse streamVarianceMetrics(StreamVarianceMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamVarianceMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getContext().getTraceId(), "StreamVarianceMetrics");

        var outbound0 = ResolveVarianceConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveVarianceConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getSubscriber().getName())
                                .setPhone(request.getSubscriber().getPhone())
                                .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> budgetingVarianceSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveVarianceConflict(outbound0), rpcExecutor);

        var outbound1 = ListVarianceImportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListVarianceImportsRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setRegion(request.getFilter().getRegion())
                        .addAllStatuses(request.getFilter().getStatusesList())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> budgetingVarianceImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listVarianceImports(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = RecordAuditTrailVersionRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getVariance().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecordAuditTrailVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordAuditTrailVersion(value));

        var outbound3 = BeanCopies.copy(AdvanceFixedAssetWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AdvanceFixedAssetWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getVariance().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(reply2.getAuditTrail().getBillingAddress().getRegion())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getVariance().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getVariance().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build(), AdvanceFixedAssetWorkflowRequest.class);
        var reply3 = assetsFixedAssetWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceFixedAssetWorkflow(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamVarianceMetrics", request.getFilter().getQuery());
        return StreamVarianceMetricsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setUserId(reply0.getVariance().getAssignee().getUserId())
                                .build())
                        .addComments(VarianceComment.newBuilder()
                                .setCreatedAt(reply2.getAuditTrail().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply3.getFixedAsset().getOwner().getEmail())
                                .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                .build())
                        .setSla(VarianceSla.newBuilder()
                                .setDueAt(reply2.getAuditTrail().getDueAt())
                                .build())
                        .setStatus(reply0.getVariance().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists variances a page at a time.
     */
    public ListVarianceReportsResponse listVarianceReports(ListVarianceReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListVarianceReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getContext().getTraceId(), "ListVarianceReports");

        var outbound0 = UpdateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setOpenedAt(request.getFilter().getCreatedAfter())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.budgeting.v1.BudgetService");
        var selected0 = BudgetServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.updateBudget(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListVarianceReports", request.getPage().getPageToken());
        return ListVarianceReportsResponse.newBuilder()
                .addVariances(Variance.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply0.getBudget().getContact().getPhone())
                                .build())
                        .build())
                .build();
    }

    /**
     * Shares a variance.
     */
    public ShareVarianceReportResponse shareVarianceReport(ShareVarianceReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareVarianceReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "ShareVarianceReport");

        var outbound0 = CancelAttestationWorkflowRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(CancelAttestationWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var command0 = new DispatchCommand<>("acme.compliance.v1.AttestationWorkflowService/CancelAttestationWorkflow", outbound0);
        var reply0 = complianceAttestationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelAttestationWorkflow(command0.payload());

        var outbound1 = mapper.shareVarianceReportStage1(request, reply0);
        var reply1 = complianceComplianceCaseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallComplianceCase(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareVarianceReport", request.getDetails().getRecipient().getPhone());
        return ShareVarianceReportResponse.newBuilder()
                .setOutcome(ShareVarianceReportResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getComplianceCase().getAudit().getUpdatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getComplianceCase().getRequester().getEmail())
                                .setName(request.getDetails().getRecipient().getName())
                                .build())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Counts variances by status.
     */
    public CountVarianceTotalsResponse countVarianceTotals(CountVarianceTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountVarianceTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getContext().getTraceId(), "CountVarianceTotals");

        var outbound0 = BatchGetSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            BatchGetSuppliersResponse fetch(BatchGetSuppliersRequest input) {
                return suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetSuppliers(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountVarianceTotals", request.getPage().getPageToken());
        return CountVarianceTotalsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Deletes a variance.
     */
    public DeleteVarianceReportResponse deleteVarianceReport(DeleteVarianceReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteVarianceReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "DeleteVarianceReport");

        var outbound0 = PreviewDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewDocumentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setDocument(Document.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewDocument(value)).orElseThrow();

        var outbound1 = CompareSoftwareLicenseOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareSoftwareLicenseOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(reply0.getDocument().getBillingAddress().getRecipient())
                                .setRegion(reply0.getDocument().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setSoftwareLicense(SoftwareLicense.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getDocument().getAudit().getCreatedBy())
                                .build())
                        .setBank(SoftwareLicenseBankFunding.newBuilder()
                                .setAccountHolder(reply0.getDocument().getBank().getAccountHolder())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.licensing.v1.SoftwareLicenseCalculationService");
        var selected1 = SoftwareLicenseCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.compareSoftwareLicenseOptions(outbound1);

        var outbound2 = SuggestAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getSoftwareLicense().getAudit().getCreatedAt())
                        .putAllLabels(reply1.getSoftwareLicense().getLabelsMap())
                        .setRegion(reply0.getDocument().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending2 = auditAuditEventQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).suggestAuditEvents(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteVarianceReport", request.getVarianceId());
        return DeleteVarianceReportResponse.newBuilder()
                .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
