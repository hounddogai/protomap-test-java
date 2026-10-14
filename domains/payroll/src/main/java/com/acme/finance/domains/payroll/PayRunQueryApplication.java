package com.acme.finance.domains.payroll;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.WatchAccessPoliciesRequest;
import com.acme.authorization.v1.WatchAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountVersionsRequest;
import com.acme.ledger.v1.ListLedgerAccountVersionsResponse;
import com.acme.payroll.v1.BrowsePayRunsRequest;
import com.acme.payroll.v1.BrowsePayRunsResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.ExportPayRunsRequest;
import com.acme.payroll.v1.ExportPayRunsResponse;
import com.acme.payroll.v1.FindPayRunsRequest;
import com.acme.payroll.v1.FindPayRunsResponse;
import com.acme.payroll.v1.GetPayRunRatesRequest;
import com.acme.payroll.v1.GetPayRunRatesResponse;
import com.acme.payroll.v1.LintPayStubsRequest;
import com.acme.payroll.v1.LintPayStubsResponse;
import com.acme.payroll.v1.LookupPayRunRequest;
import com.acme.payroll.v1.LookupPayRunResponse;
import com.acme.payroll.v1.NormalizeDeductionRequest;
import com.acme.payroll.v1.NormalizeDeductionResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubBankSettlement;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.PeekPayRunRequest;
import com.acme.payroll.v1.PeekPayRunResponse;
import com.acme.payroll.v1.QuoteTaxWithholdingRequest;
import com.acme.payroll.v1.QuoteTaxWithholdingResponse;
import com.acme.payroll.v1.ResolvePayRunsRequest;
import com.acme.payroll.v1.ResolvePayRunsResponse;
import com.acme.payroll.v1.SimulateTaxWithholdingRequest;
import com.acme.payroll.v1.SimulateTaxWithholdingResponse;
import com.acme.payroll.v1.SuggestPayRunsRequest;
import com.acme.payroll.v1.SuggestPayRunsResponse;
import com.acme.payroll.v1.SummarizePayRunsRequest;
import com.acme.payroll.v1.SummarizePayRunsResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingBlockAction;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.workplaces.v1.BrowseBuildingsRequest;
import com.acme.workplaces.v1.BrowseBuildingsResponse;
import com.acme.workplaces.v1.BuildingQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about pay runs, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PayRunQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(PayRunQueryApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking;

    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking;

    private final PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking;

    private final TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking;

    private final PayrollRepository repository;

    private final BuildingQueryServiceGrpc.BuildingQueryServiceBlockingStub workplacesBuildingQueryServiceBlocking;

    public PayRunQueryApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking,
            PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking,
            PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking,
            TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking,
            PayrollRepository repository,
            BuildingQueryServiceGrpc.BuildingQueryServiceBlockingStub workplacesBuildingQueryServiceBlocking) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseServiceAsync = complianceComplianceCaseServiceAsync;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.payrollPayRunCalculationServiceBlocking = payrollPayRunCalculationServiceBlocking;
        this.payrollPayStubCalculationServiceBlocking = payrollPayStubCalculationServiceBlocking;
        this.payrollPayStubValidationServiceBlocking = payrollPayStubValidationServiceBlocking;
        this.payrollTaxWithholdingCalculationServiceBlocking = payrollTaxWithholdingCalculationServiceBlocking;
        this.repository = repository;
        this.workplacesBuildingQueryServiceBlocking = workplacesBuildingQueryServiceBlocking;
    }

    /**
     * Returns a pay run.
     */
    public LookupPayRunResponse lookupPayRun(LookupPayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupPayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getPayRunId(), "LookupPayRun");

        var outbound0 = ListLedgerAccountVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLedgerAccountVersionsRequest.Filter.newBuilder()
                        .setOwnerId(request.getPayRunId())
                        .build())
                .build();
        RpcPort<ListLedgerAccountVersionsRequest, ListLedgerAccountVersionsResponse> port0 = RpcPort.adapt(ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listLedgerAccountVersions);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = BatchCreateComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateComplianceCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
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
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        return LookupPayRunResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists pay runs a page at a time.
     */
    public BrowsePayRunsResponse browsePayRuns(BrowsePayRunsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowsePayRuns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getContext().getTraceId(), "BrowsePayRuns");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowsePayRuns", request.getPage().getPageToken());
        return BrowsePayRunsResponse.getDefaultInstance();
    }

    /**
     * Lists pay runs a page at a time.
     */
    public FindPayRunsResponse findPayRuns(FindPayRunsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindPayRuns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getContext().getTraceId(), "FindPayRuns");

        var outbound0 = WatchAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var stream0 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchAccessPolicies(outbound0);
        var reply0 = WatchAccessPoliciesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindPayRuns", request.getPage().getPageToken());
        return FindPayRunsResponse.getDefaultInstance();
    }

    /**
     * Returns several pay runs at once.
     */
    public ResolvePayRunsResponse resolvePayRuns(ResolvePayRunsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolvePayRuns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getContext().getTraceId(), "ResolvePayRuns");
        return ResolvePayRunsResponse.getDefaultInstance();
    }

    /**
     * Streams every matching pay run.
     */
    public ExportPayRunsResponse exportPayRuns(ExportPayRunsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportPayRuns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getContext().getTraceId(), "ExportPayRuns");

        var outbound0 = RpcSupport.unpack(Any.pack(LintPayStubsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintPayStubsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build()), LintPayStubsRequest.class);
        var reply0 = payrollPayStubValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintPayStubs(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportPayRuns", request.getFilter().getQuery());
        return ExportPayRunsResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPayStub().getBillingAddress().getCountryCode())
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .putAllMetadata(reply0.getPayStub().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Summarizes a pay run.
     */
    public SummarizePayRunsResponse summarizePayRuns(SummarizePayRunsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizePayRuns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getPayRunId(), "SummarizePayRuns");

        var outbound0 = GetPayRunRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRunId(request.getPayRunId())
                .build();
        final class Stage0Adapter {
            GetPayRunRatesResponse fetch(GetPayRunRatesRequest input) {
                return payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getPayRunRates(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = ComparePayStubOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ComparePayStubOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply0.getPayRun().getBillingAddress().getLocality())
                                .setRecipient(reply0.getPayRun().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setBank(PayStubBankSettlement.newBuilder()
                                .setAccountHolder(switch (reply0.getPayRun().getSettlementCase()) {
                                    case BANK -> reply0.getPayRun().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPayRun().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply1 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePayStubOptions(outbound1);

        var outbound2 = (SimulateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateTaxWithholdingRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply0.getPayRun().getBillingAddress().getLocality())
                                .setRegion(reply0.getPayRun().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPayRun().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateTaxWithholding(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizePayRuns", request.getDetails().getFormat());
        return SummarizePayRunsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists pay runs a page at a time.
     */
    public SuggestPayRunsResponse suggestPayRuns(SuggestPayRunsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestPayRuns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getContext().getTraceId(), "SuggestPayRuns");

        var outbound0 = BeanCopies.copy(BrowseBuildingsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseBuildingsRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build(), BrowseBuildingsRequest.class);
        var reply0 = workplacesBuildingQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseBuildings(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestPayRuns", request.getPage().getPageToken());
        return SuggestPayRunsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply0.getPageInfo().getNextPageToken())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a pay run.
     */
    public PeekPayRunResponse peekPayRun(PeekPayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekPayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getPayRunId(), "PeekPayRun");

        var outbound0 = NormalizeDeductionRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setDetails(NormalizeDeductionRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.payroll.v1.DeductionValidationService");
        var selected0 = DeductionValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.normalizeDeduction(outbound0);

        var outbound1 = QuoteTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getDeduction().getAudit().getCreatedAt())
                                .build())
                        .setBlock(TaxWithholdingBlockAction.newBuilder()
                                .setReason(switch (reply0.getDeduction().getActionCase()) {
                                    case BLOCK -> reply0.getDeduction().getBlock().getReason();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteTaxWithholding(outbound1);
        return PeekPayRunResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .addLines(PayRunLine.newBuilder()
                                .setAmount(reply0.getDeduction().getDiscount().getAmount())
                                .setTax(PayRunLineTax.newBuilder()
                                        .setAmount(reply0.getDeduction().getDiscount().getAmount())
                                        .build())
                                .build())
                        .setPayRunId(request.getPayRunId())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .build();
    }
}
