package com.acme.finance.domains.insurance;

import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.ExportAccessPoliciesRequest;
import com.acme.authorization.v1.ExportAccessPoliciesResponse;
import com.acme.billing.v1.BrowseInvoicesRequest;
import com.acme.billing.v1.BrowseInvoicesResponse;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.RecalculatePaymentTermsRequest;
import com.acme.billing.v1.RecalculatePaymentTermsResponse;
import com.acme.billing.v1.SummarizeInvoicesRequest;
import com.acme.billing.v1.SummarizeInvoicesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.NormalizeHouseholdRequest;
import com.acme.customers.v1.NormalizeHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.BrowsePoliciesRequest;
import com.acme.insurance.v1.BrowsePoliciesResponse;
import com.acme.insurance.v1.CancelUnderwritingImportRequest;
import com.acme.insurance.v1.CancelUnderwritingImportResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.ExportPoliciesRequest;
import com.acme.insurance.v1.ExportPoliciesResponse;
import com.acme.insurance.v1.GetUnderwritingImportStatusRequest;
import com.acme.insurance.v1.GetUnderwritingImportStatusResponse;
import com.acme.insurance.v1.ImportUnderwritingsRequest;
import com.acme.insurance.v1.ImportUnderwritingsResponse;
import com.acme.insurance.v1.ListUnderwritingImportsRequest;
import com.acme.insurance.v1.ListUnderwritingImportsResponse;
import com.acme.insurance.v1.MapUnderwritingImportColumnsRequest;
import com.acme.insurance.v1.MapUnderwritingImportColumnsResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.PreviewClaimRequest;
import com.acme.insurance.v1.PreviewClaimResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSla;
import com.acme.insurance.v1.ValidateUnderwritingImportRequest;
import com.acme.insurance.v1.ValidateUnderwritingImportResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.NotifyPostingBatchWatchersRequest;
import com.acme.ledger.v1.NotifyPostingBatchWatchersResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesResponse;
import com.acme.ratelimiting.v1.ExplainRateLimitCalculationRequest;
import com.acme.ratelimiting.v1.ExplainRateLimitCalculationResponse;
import com.acme.ratelimiting.v1.RateLimit;
import com.acme.ratelimiting.v1.RateLimitCalculationServiceGrpc;
import com.acme.workplaces.v1.Building;
import com.acme.workplaces.v1.BuildingCalculationServiceGrpc;
import com.acme.workplaces.v1.SimulateBuildingRequest;
import com.acme.workplaces.v1.SimulateBuildingResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Takes underwritings in bulk from partners and legacy systems. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class UnderwritingImportApplication {
    private static final Logger log = LoggerFactory.getLogger(UnderwritingImportApplication.class);

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final InvoiceQueryServiceGrpc.InvoiceQueryServiceBlockingStub billingInvoiceQueryServiceBlocking;

    private final PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking;

    private final PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final UnderwritingImportRequestMapper mapper;

    private final RateLimitCalculationServiceGrpc.RateLimitCalculationServiceFutureStub ratelimitingRateLimitCalculationServiceFuture;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final BuildingCalculationServiceGrpc.BuildingCalculationServiceBlockingStub workplacesBuildingCalculationServiceBlocking;

    public UnderwritingImportApplication(
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            InvoiceQueryServiceGrpc.InvoiceQueryServiceBlockingStub billingInvoiceQueryServiceBlocking,
            PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking,
            ChannelRegistry channels,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking,
            PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            UnderwritingImportRequestMapper mapper,
            RateLimitCalculationServiceGrpc.RateLimitCalculationServiceFutureStub ratelimitingRateLimitCalculationServiceFuture,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            BuildingCalculationServiceGrpc.BuildingCalculationServiceBlockingStub workplacesBuildingCalculationServiceBlocking) {
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.billingInvoiceQueryServiceBlocking = billingInvoiceQueryServiceBlocking;
        this.billingPaymentTermCalculationServiceBlocking = billingPaymentTermCalculationServiceBlocking;
        this.channels = channels;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.insuranceClaimValidationServiceBlocking = insuranceClaimValidationServiceBlocking;
        this.insurancePolicyQueryServiceBlocking = insurancePolicyQueryServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.mapper = mapper;
        this.ratelimitingRateLimitCalculationServiceFuture = ratelimitingRateLimitCalculationServiceFuture;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.workplacesBuildingCalculationServiceBlocking = workplacesBuildingCalculationServiceBlocking;
    }

    /**
     * Takes a stream of underwritings and answers once at the end.
     */
    public ImportUnderwritingsResponse importUnderwritings(ImportUnderwritingsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ImportUnderwritings")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveUnderwriting(request.getContext().getTenantId(), request.getRecord());
        }

        var results0 = new ArrayList<BrowsePoliciesResponse>();
        for (var element : request.getRecord().getHistoryList()) {
            BrowsePoliciesRequest itemRequest = BrowsePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowsePoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getChangedAt())
                        .putAllLabels(request.getRecord().getCustomFieldsMap())
                        .setOwnerId(request.getRecord().getCustomer().getClaimId())
                        .build())
                .build();
            results0.add(insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browsePolicies(itemRequest));
        }
        var reply0 = results0.isEmpty() ? BrowsePoliciesResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = PreviewClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getAudit().getEtag();
                                    default -> "";
                                })
                                .setUpdatedBy(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getAudit().getUpdatedBy();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewClaim(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ImportUnderwritings", request.getHeader().getSource());
        return ImportUnderwritingsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns an underwriting.
     */
    public GetUnderwritingImportStatusResponse getUnderwritingImportStatus(GetUnderwritingImportStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetUnderwritingImportStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getUnderwritingId(), "GetUnderwritingImportStatus");

        var outbound0 = SummarizeInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SummarizeInvoicesRequest.Details.newBuilder()
                        .putAllFilters(request.getContext().getBaggageMap())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.billing.v1.InvoiceQueryService");
        var serviceName0 = "acme.billing.v1" + "." + "InvoiceQueryService";
        var methodName0 = "Sum" + "marizeInvoices";
        var descriptor0 = MethodDescriptor.<SummarizeInvoicesRequest, SummarizeInvoicesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(SummarizeInvoicesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SummarizeInvoicesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        return GetUnderwritingImportStatusResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Cancels an underwriting.
     */
    public CancelUnderwritingImportResponse cancelUnderwritingImport(CancelUnderwritingImportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelUnderwritingImport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getUnderwritingId(), "CancelUnderwritingImport");

        var outbound0 = NormalizeHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.customers.v1.HouseholdValidationService/NormalizeHousehold", outbound0);
        var reply0 = customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeHousehold(command0.payload());

        var outbound1 = BrowseInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseInvoicesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getHousehold().getAudit().getUpdatedAt())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .setRegion(reply0.getHousehold().getLocation().getRegion())
                        .build())
                .build();
        final class Stage1Adapter {
            BrowseInvoicesResponse fetch(BrowseInvoicesRequest input) {
                return billingInvoiceQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseInvoices(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelUnderwritingImport", request.getDetails().getReasonCode());
        return CancelUnderwritingImportResponse.newBuilder()
                .setOutcome(CancelUnderwritingImportResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Validates an underwriting.
     */
    public ValidateUnderwritingImportResponse validateUnderwritingImport(ValidateUnderwritingImportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateUnderwritingImport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = NotifyPostingBatchWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyPostingBatchWatchersRequest.Details.newBuilder()
                        .setChannel(switch (request.getUnderwriting().getOriginCase()) {
                            case CHANNEL -> request.getUnderwriting().getChannel().getChannel();
                            default -> "";
                        })
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getUnderwriting().getAssignee().getEmail())
                                .setPhone(request.getUnderwriting().getRequester().getPhone())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyPostingBatchWatchers(outbound0);

        var outbound1 = ExportAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPostingBatch().getDueAt())
                        .setOwnerId(request.getUnderwriting().getAssignee().getUserId())
                        .setRegion(reply0.getPostingBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportAccessPolicies(outbound1);
        var reply1 = ExportAccessPoliciesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateUnderwritingImport", request.getUnderwriting().getUnderwritingId());
        return ValidateUnderwritingImportResponse.newBuilder()
                .setUnderwriting(Underwriting.newBuilder()
                        .setSla(UnderwritingSla.newBuilder()
                                .setDueAt(reply0.getPostingBatch().getDueAt())
                                .build())
                        .setSubject(request.getUnderwriting().getSubject())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }

    /**
     * Lists underwritings a page at a time.
     */
    public ListUnderwritingImportsResponse listUnderwritingImports(ListUnderwritingImportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListUnderwritingImports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Underwriting", request.getContext().getTraceId(), "ListUnderwritingImports");

        var outbound0 = mapper.listUnderwritingImportsStage0(request);
        var reply0 = billingPaymentTermCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePaymentTerms(outbound0);

        var outbound1 = SimulateBuildingRequest.newBuilder()
                .setBuilding(Building.newBuilder()
                        .addChildren(Building.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setUpdatedAt(reply0.getPaymentTerm().getAudit().getUpdatedAt())
                                        .build())
                                .addChildren(Building.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(reply0.getPaymentTerm().getAudit().getCreatedBy())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply0.getPaymentTerm().getOwner().getEmail())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(reply0.getPaymentTerm().getOwner().getDisplayName())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage1Adapter {
            SimulateBuildingResponse fetch(SimulateBuildingRequest input) {
                return workplacesBuildingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateBuilding(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListUnderwritingImports", request.getPage().getPageToken());
        return ListUnderwritingImportsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .addUnderwritings(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPaymentTerm().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getBuilding().getManager().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Maps an underwriting.
     */
    public MapUnderwritingImportColumnsResponse mapUnderwritingImportColumns(MapUnderwritingImportColumnsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "MapUnderwritingImportColumns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveUnderwriting(request.getContext().getTenantId(), request.getUnderwriting());

        var outbound0 = ExportPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getUnderwriting().getAudit().getUpdatedAt())
                        .build())
                .build();
        var stream0 = insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportPolicies(outbound0);
        var reply0 = ExportPoliciesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = ExplainRateLimitCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainRateLimitCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setRateLimit(RateLimit.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getUnderwriting().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getUnderwriting().getCustomer().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending1 = ratelimitingRateLimitCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).explainRateLimitCalculation(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var futures2 = request.getUnderwriting().getCommentsList().stream()
                .map(element -> SubscribeFiscalPeriodUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeFiscalPeriodUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .setName(request.getUnderwriting().getRequester().getName())
                                .setPhone(request.getUnderwriting().getRequester().getPhone())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeFiscalPeriodUpdates(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? SubscribeFiscalPeriodUpdatesResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getUnderwriting().getUnderwritingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "MapUnderwritingImportColumns", request.getUnderwriting().getUnderwritingId());
        return MapUnderwritingImportColumnsResponse.newBuilder()
                .setOutcome(MapUnderwritingImportColumnsResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setPriority(request.getUnderwriting().getPriority())
                        .setUnderwritingId(request.getUnderwritingId())
                        .build())
                .build();
    }
}
