package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.CalculateAllocationRequest;
import com.acme.accounting.v1.CalculateAllocationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityRequest;
import com.acme.accounting.v1.CheckChartOfAccountsEligibilityResponse;
import com.acme.accounting.v1.CompareAllocationOptionsRequest;
import com.acme.accounting.v1.CompareAllocationOptionsResponse;
import com.acme.accounting.v1.EstimateAllocationRequest;
import com.acme.accounting.v1.EstimateAllocationResponse;
import com.acme.accounting.v1.GetAllocationRatesRequest;
import com.acme.accounting.v1.GetAllocationRatesResponse;
import com.acme.accounting.v1.RecalculateAllocationsRequest;
import com.acme.accounting.v1.RecalculateAllocationsResponse;
import com.acme.accounting.v1.ScoreChartOfAccountsRequest;
import com.acme.accounting.v1.ScoreChartOfAccountsResponse;
import com.acme.accounting.v1.SimulateAllocationRequest;
import com.acme.accounting.v1.SimulateAllocationResponse;
import com.acme.accounting.v1.ValidateChartOfAccountsRequest;
import com.acme.accounting.v1.ValidateChartOfAccountsResponse;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.LintGoodsReceiptsRequest;
import com.acme.procurement.v1.LintGoodsReceiptsResponse;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.venues.v1.SummarizeVenuesRequest;
import com.acme.venues.v1.SummarizeVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of an allocation, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class AllocationCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(AllocationCalculationApplication.class);

    private final ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking;

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceBlockingStub procurementGoodsReceiptValidationServiceBlocking;

    private final AccountingRepository repository;

    private final RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public AllocationCalculationApplication(
            ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking,
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            ChannelRegistry channels,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceBlockingStub procurementGoodsReceiptValidationServiceBlocking,
            AccountingRepository repository,
            RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.accountingChartOfAccountsValidationServiceBlocking = accountingChartOfAccountsValidationServiceBlocking;
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.channels = channels;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.procurementGoodsReceiptValidationServiceBlocking = procurementGoodsReceiptValidationServiceBlocking;
        this.repository = repository;
        this.riskRiskScenarioQueryServiceBlocking = riskRiskScenarioQueryServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    /**
     * Calculates an allocation.
     */
    public CalculateAllocationResponse calculateAllocation(CalculateAllocationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateAllocation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = ShareRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareRetentionPolicyExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getAllocation().getOwner().getEmail())
                                .build())
                        .build())
                .setReason(switch (request.getAllocation().getActionCase()) {
                    case BLOCK -> request.getAllocation().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply0 = auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareRetentionPolicyExport(outbound0);

        var outbound1 = ValidateChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply0.getRetentionPolicy().getAudit().getCreatedAt())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getAllocation().getOwner().getEmail())
                                                .setUserId(reply0.getRetentionPolicy().getOwner().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.accounting.v1.ChartOfAccountsValidationService");
        var selected1 = ChartOfAccountsValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.validateChartOfAccounts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateAllocation", request.getAllocation().getAllocationId());
        return CalculateAllocationResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRetentionPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setName(reply1.getChartOfAccounts().getName())
                        .setPriority(request.getAllocation().getPriority())
                        .build())
                .setBreakdown(CalculateAllocationResponse.Breakdown.newBuilder()
                        .addLines(CalculateAllocationResponse.Breakdown.Line.newBuilder()
                                .setAmount(reply0.getRetentionPolicy().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
    }

    /**
     * Estimates an allocation.
     */
    public EstimateAllocationResponse estimateAllocation(EstimateAllocationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateAllocation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = SummarizeVenuesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getAllocation().getBlock().getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeVenues(outbound0), rpcExecutor);

        var outbound1 = CreateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getAllocation().getAudit().getEtag())
                                .setUpdatedBy(request.getAllocation().getAudit().getUpdatedBy())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setUserId(request.getAllocation().getOwner().getUserId())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createUser(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = ShareRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareRetentionPolicyExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply1.getUser().getRelationshipManager().getEmail())
                                .setName(reply1.getUser().getContact().getName())
                                .setPreferredChannel(reply1.getUser().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.audit.v1.RetentionPolicyExportService");
        var serviceName2 = "acme.audit.v1" + "." + "RetentionPolicyExportService";
        var methodName2 = "Sha" + "reRetentionPolicyExport";
        var descriptor2 = MethodDescriptor.<ShareRetentionPolicyExportRequest, ShareRetentionPolicyExportResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ShareRetentionPolicyExportRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ShareRetentionPolicyExportResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateAllocation", request.getAllocation().getAllocationId());
        return EstimateAllocationResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getRetentionPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setEffective(reply2.getRetentionPolicy().getEffective())
                        .build())
                .setBreakdown(EstimateAllocationResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Compares an allocation.
     */
    public CompareAllocationOptionsResponse compareAllocationOptions(CompareAllocationOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareAllocationOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = ExplainStandingOrderDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAllocation().getAudit().getCreatedAt())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(request.getAllocation().getOwner().getDisplayName())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getAllocation().getOwner().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ExplainStandingOrderDecisionRequest, ExplainStandingOrderDecisionResponse>() {
            @Override
            public ExplainStandingOrderDecisionResponse apply(ExplainStandingOrderDecisionRequest input) {
                return bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainStandingOrderDecision(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = PreviewMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewMandateRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getAllocation().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.banking.v1.MandateValidationService");
        var selected1 = MandateValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.previewMandate(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareAllocationOptions", request.getAllocation().getAllocationId());
        return CompareAllocationOptionsResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setVersion(request.getAllocation().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalculates an allocation.
     */
    public RecalculateAllocationsResponse recalculateAllocations(RecalculateAllocationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateAllocations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = CheckChartOfAccountsEligibilityRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getAllocation().getAudit().getCreatedAt())
                                                .setUpdatedBy(request.getAllocation().getAudit().getUpdatedBy())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setLocality(request.getDetails().getDestination().getLocality())
                                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.accounting.v1.ChartOfAccountsValidationService");
        var selected0 = ChartOfAccountsValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.checkChartOfAccountsEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateAllocations", request.getAllocation().getAllocationId());
        return RecalculateAllocationsResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getChartOfAccounts().getAudit().getCreatedAt())
                                .build())
                        .setOwner(request.getAllocation().getOwner())
                        .build())
                .build();
    }

    /**
     * Simulates an allocation.
     */
    public SimulateAllocationResponse simulateAllocation(SimulateAllocationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateAllocation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = (CountRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountRiskScenariosRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAllocation().getAudit().getCreatedAt())
                        .putAllLabels(request.getAllocation().getParametersMap())
                        .setOwnerId(request.getAllocation().getOwner().getUserId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply0 = riskRiskScenarioQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countRiskScenarios(outbound0);

        var builder1 = LintGoodsReceiptsRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setGoodsReceipt(GoodsReceipt.newBuilder()
                .setBillingAddress(PostalAddress.newBuilder()
                        .setLocality(request.getDetails().getDestination().getLocality())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = procurementGoodsReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintGoodsReceipts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateAllocation", request.getAllocation().getAllocationId());
        return SimulateAllocationResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setEffective(request.getAllocation().getEffective())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns an allocation.
     */
    public GetAllocationRatesResponse getAllocationRates(GetAllocationRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAllocationRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Allocation", request.getAllocationId(), "GetAllocationRates");

        var outbound0 = RpcSupport.unpack(Any.pack(ScoreChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getContext().getDeadline())
                                                .build())
                                        .addChildren(ChartOfAccounts.newBuilder()
                                                .setParentId(request.getAllocationId())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()), ScoreChartOfAccountsRequest.class);
        var reply0 = accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreChartOfAccounts(outbound0);

        var outbound1 = BrowseSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getChartOfAccounts().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getChartOfAccounts().getAttributesMap())
                        .setOwnerId(reply0.getChartOfAccounts().getParentId())
                        .setRegion(reply0.getChartOfAccounts().getLocation().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSearchIndexes(value));
        return GetAllocationRatesResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getChartOfAccounts().getAudit().getEtag())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
