package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.AccrualQueryServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesResponse;
import com.acme.accounting.v1.BatchGetChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchGetChartOfAccountsesResponse;
import com.acme.accounting.v1.BrowseAccrualsRequest;
import com.acme.accounting.v1.BrowseAccrualsResponse;
import com.acme.accounting.v1.CalculateReconciliationRequest;
import com.acme.accounting.v1.CalculateReconciliationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsRef;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.CompareAllocationOptionsRequest;
import com.acme.accounting.v1.CompareAllocationOptionsResponse;
import com.acme.accounting.v1.CreateChartOfAccountsRequest;
import com.acme.accounting.v1.CreateChartOfAccountsResponse;
import com.acme.accounting.v1.DeleteChartOfAccountsRequest;
import com.acme.accounting.v1.DeleteChartOfAccountsResponse;
import com.acme.accounting.v1.EstimateAllocationRequest;
import com.acme.accounting.v1.EstimateAllocationResponse;
import com.acme.accounting.v1.ExplainReconciliationCalculationRequest;
import com.acme.accounting.v1.ExplainReconciliationCalculationResponse;
import com.acme.accounting.v1.GetChartOfAccountsRequest;
import com.acme.accounting.v1.GetChartOfAccountsResponse;
import com.acme.accounting.v1.ListChartOfAccountsesRequest;
import com.acme.accounting.v1.ListChartOfAccountsesResponse;
import com.acme.accounting.v1.PreviewReconciliationRequest;
import com.acme.accounting.v1.PreviewReconciliationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.accounting.v1.ScoreChartOfAccountsRequest;
import com.acme.accounting.v1.ScoreChartOfAccountsResponse;
import com.acme.accounting.v1.SuggestAccrualsRequest;
import com.acme.accounting.v1.SuggestAccrualsResponse;
import com.acme.accounting.v1.UpdateChartOfAccountsRequest;
import com.acme.accounting.v1.UpdateChartOfAccountsResponse;
import com.acme.accounting.v1.WatchChartOfAccountsesRequest;
import com.acme.accounting.v1.WatchChartOfAccountsesResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.PreviewFixedAssetRequest;
import com.acme.assets.v1.PreviewFixedAssetResponse;
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
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.LintPostingBatchesRequest;
import com.acme.ledger.v1.LintPostingBatchesResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Manages chart of accountses: creates, reads, changes, and lists them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ChartOfAccountsApplication {
    private static final Logger log = LoggerFactory.getLogger(ChartOfAccountsApplication.class);

    private final AccrualQueryServiceGrpc.AccrualQueryServiceBlockingStub accountingAccrualQueryServiceBlocking;

    private final AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking;

    private final ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking;

    private final ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking;

    private final ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceFutureStub accountingReconciliationCalculationServiceFuture;

    private final ReconciliationValidationServiceGrpc.ReconciliationValidationServiceFutureStub accountingReconciliationValidationServiceFuture;

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final ChartOfAccountsRequestMapper mapper;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking;

    private final AccountingRepository repository;

    public ChartOfAccountsApplication(
            AccrualQueryServiceGrpc.AccrualQueryServiceBlockingStub accountingAccrualQueryServiceBlocking,
            AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking,
            ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking,
            ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking,
            ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceFutureStub accountingReconciliationCalculationServiceFuture,
            ReconciliationValidationServiceGrpc.ReconciliationValidationServiceFutureStub accountingReconciliationValidationServiceFuture,
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            ChartOfAccountsRequestMapper mapper,
            MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking,
            AccountingRepository repository) {
        this.accountingAccrualQueryServiceBlocking = accountingAccrualQueryServiceBlocking;
        this.accountingAllocationCalculationServiceBlocking = accountingAllocationCalculationServiceBlocking;
        this.accountingChartOfAccountsValidationServiceBlocking = accountingChartOfAccountsValidationServiceBlocking;
        this.accountingReconciliationCalculationServiceBlocking = accountingReconciliationCalculationServiceBlocking;
        this.accountingReconciliationCalculationServiceFuture = accountingReconciliationCalculationServiceFuture;
        this.accountingReconciliationValidationServiceFuture = accountingReconciliationValidationServiceFuture;
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.complianceComplianceCaseServiceAsync = complianceComplianceCaseServiceAsync;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesQueryServiceBlocking = metricsMetricSeriesQueryServiceBlocking;
        this.repository = repository;
    }

    /**
     * Creates a chart of accounts.
     */
    public CreateChartOfAccountsResponse createChartOfAccounts(CreateChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var builder0 = BrowseAccrualsRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setFilter(BrowseAccrualsRequest.Filter.newBuilder()
                .setCreatedAfter(request.getChartOfAccounts().getAudit().getCreatedAt())
                .setRegion(request.getChartOfAccounts().getLocation().getRegion())
                .build());
        var outbound0 = builder0.build();
        var reply0 = accountingAccrualQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseAccruals(outbound0);

        var outbound1 = BatchCreateComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateComplianceCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getChartOfAccounts().getChartOfAccountsId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getChartOfAccounts().getManager().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateComplianceCasesResponse.getDefaultInstance());
        var input1 = complianceComplianceCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateComplianceCases(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getChartOfAccounts().getAudit().getCreatedAt())
                                .setCreatedBy(request.getChartOfAccounts().getAudit().getCreatedBy())
                                .setEtag(request.getChartOfAccounts().getAudit().getEtag())
                                .setUpdatedAt(request.getChartOfAccounts().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateChartOfAccounts", request.getChartOfAccounts().getName());
        return CreateChartOfAccountsResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setLocation(request.getChartOfAccounts().getLocation())
                        .setManager(request.getChartOfAccounts().getManager())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a chart of accounts.
     */
    public GetChartOfAccountsResponse getChartOfAccounts(GetChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getChartOfAccountsId(), "GetChartOfAccounts");

        var outbound0 = PreviewReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setCustomer(ChartOfAccountsRef.newBuilder()
                                .setChartOfAccountsId(request.getChartOfAccountsId())
                                .build())
                        .build())
                .build();
        var pending0 = accountingReconciliationValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).previewReconciliation(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = BeanCopies.copy(CalculateReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getReconciliation().getAssignee().getDisplayName())
                                .setEmail(reply0.getReconciliation().getRequester().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getReconciliation().getAudit().getEtag())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getReconciliation().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build(), CalculateReconciliationRequest.class);
        var reply1 = accountingReconciliationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateReconciliation(outbound1);
        return GetChartOfAccountsResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(reply0.getReconciliation().getAudit().getUpdatedAt())
                                                .build())
                                        .addChildren(ChartOfAccounts.newBuilder()
                                                .setChartOfAccountsId(reply0.getReconciliation().getCustomer().getChartOfAccountsId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    /**
     * Changes a chart of accounts.
     */
    public UpdateChartOfAccountsResponse updateChartOfAccounts(UpdateChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getChartOfAccounts());

        var outbound0 = mapper.updateChartOfAccountsStage0(request);
        var reply0 = metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseMetricSerieses(outbound0);

        var outbound1 = PreviewFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getChartOfAccounts().getLocation().getCountryCode())
                                .setPostalCode(request.getChartOfAccounts().getLocation().getPostalCode())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getChartOfAccounts().getManager().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewFixedAsset(value)).orElseThrow();

        var builder2 = EstimateAllocationRequest.newBuilder();
        builder2.setAllocation(Allocation.newBuilder()
                .setOwner(UserRef.newBuilder()
                        .setDisplayName(request.getChartOfAccounts().getManager().getDisplayName())
                        .build())
                .build());
        builder2.setContext(request.getContext());
        var outbound2 = builder2.build();
        var reply2 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateAllocation(outbound2);

        var outbound3 = CompareAllocationOptionsRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getChartOfAccounts().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getFixedAsset().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage3Adapter {
            CompareAllocationOptionsResponse fetch(CompareAllocationOptionsRequest input) {
                return accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAllocationOptions(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccounts().getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateChartOfAccounts", request.getChartOfAccounts().getChartOfAccountsId());
        return UpdateChartOfAccountsResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(reply2.getAllocation().getAudit().getEtag())
                                                .build())
                                        .build())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRecipient(reply1.getFixedAsset().getAddress().getRecipient())
                                .build())
                        .setManager(request.getChartOfAccounts().getManager())
                        .setParentId(request.getChartOfAccounts().getParentId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Deletes a chart of accounts.
     */
    public DeleteChartOfAccountsResponse deleteChartOfAccounts(DeleteChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getChartOfAccountsId(), "DeleteChartOfAccounts");

        var outbound0 = ExplainReconciliationCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainReconciliationCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .setCustomer(ChartOfAccountsRef.newBuilder()
                                .setChartOfAccountsId(request.getChartOfAccountsId())
                                .build())
                        .build())
                .build();
        var pending0 = accountingReconciliationCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).explainReconciliationCalculation(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getReason()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteChartOfAccounts", request.getReason());
        return DeleteChartOfAccountsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists chart of accountses a page at a time.
     */
    public ListChartOfAccountsesResponse listChartOfAccountses(ListChartOfAccountsesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListChartOfAccountses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getContext().getTraceId(), "ListChartOfAccountses");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListChartOfAccountses", request.getPage().getPageToken());
        return ListChartOfAccountsesResponse.getDefaultInstance();
    }

    /**
     * Returns several chart of accountses at once.
     */
    public BatchGetChartOfAccountsesResponse batchGetChartOfAccountses(BatchGetChartOfAccountsesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetChartOfAccountses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getContext().getTraceId(), "BatchGetChartOfAccountses");

        var builder0 = SuggestAccrualsRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setFilter(SuggestAccrualsRequest.Filter.newBuilder()
                .setCreatedAfter(request.getContext().getDeadline())
                .putAllLabels(request.getContext().getBaggageMap())
                .setOwnerId(request.getContext().getTraceId())
                .build());
        var outbound0 = builder0.build();
        var reply0 = accountingAccrualQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestAccruals(outbound0);
        return BatchGetChartOfAccountsesResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setTotalSize(reply0.getPageInfo().getTotalSize())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams the changes of chart of accountses as they happen.
     */
    public WatchChartOfAccountsesResponse watchChartOfAccountses(WatchChartOfAccountsesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchChartOfAccountses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getContext().getTraceId(), "WatchChartOfAccountses");

        var outbound0 = mapper.watchChartOfAccountsesStage0(request);
        var reply0 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneAccessPolicy(outbound0);

        var outbound1 = mapper.watchChartOfAccountsesStage1(request, reply0);
        var reply1 = accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreChartOfAccounts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchChartOfAccountses", request.getFilter().getQuery());
        return WatchChartOfAccountsesResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getChartOfAccounts().getAudit().getCreatedBy())
                                .build())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setUpdatedBy(reply0.getAccessPolicy().getAudit().getUpdatedBy())
                                        .build())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Takes a stream of chart of accountses and answers once at the end.
     */
    public BatchCreateChartOfAccountsesResponse batchCreateChartOfAccountses(BatchCreateChartOfAccountsesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateChartOfAccountses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveChartOfAccounts(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = LintPostingBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getRecord().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(LedgerAccountRef.newBuilder()
                                .setDisplayName(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getManager().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintPostingBatches(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateChartOfAccountses", request.getHeader().getSource());
        return BatchCreateChartOfAccountsesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
