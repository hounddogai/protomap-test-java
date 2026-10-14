package com.acme.finance.domains.lending;

import com.acme.catalog.v1.CreateProductRequest;
import com.acme.catalog.v1.CreateProductResponse;
import com.acme.catalog.v1.Product;
import com.acme.catalog.v1.ProductComponent;
import com.acme.catalog.v1.ProductServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.EstimateHouseholdRequest;
import com.acme.customers.v1.EstimateHouseholdResponse;
import com.acme.customers.v1.ExplainCustomerSegmentDecisionRequest;
import com.acme.customers.v1.ExplainCustomerSegmentDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.ValidateHouseholdRequest;
import com.acme.customers.v1.ValidateHouseholdResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.ActivateCollateralRequest;
import com.acme.lending.v1.ActivateCollateralResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.CountRepaymentTotalsRequest;
import com.acme.lending.v1.CountRepaymentTotalsResponse;
import com.acme.lending.v1.EstimateRepaymentRequest;
import com.acme.lending.v1.EstimateRepaymentResponse;
import com.acme.lending.v1.GetCollateralLifecycleRequest;
import com.acme.lending.v1.GetCollateralLifecycleResponse;
import com.acme.lending.v1.LintLoanApplicationsRequest;
import com.acme.lending.v1.LintLoanApplicationsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.NormalizeCollateralRequest;
import com.acme.lending.v1.NormalizeCollateralResponse;
import com.acme.lending.v1.NormalizeLoanRequest;
import com.acme.lending.v1.NormalizeLoanResponse;
import com.acme.lending.v1.ReinstateCollateralRequest;
import com.acme.lending.v1.ReinstateCollateralResponse;
import com.acme.lending.v1.RenewCollateralRequest;
import com.acme.lending.v1.RenewCollateralResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.SuspendCollateralRequest;
import com.acme.lending.v1.SuspendCollateralResponse;
import com.acme.lending.v1.TerminateCollateralRequest;
import com.acme.lending.v1.TerminateCollateralResponse;
import com.acme.lending.v1.ValidateRepaymentRequest;
import com.acme.lending.v1.ValidateRepaymentResponse;
import com.acme.maintenance.v1.EstimateWorkOrderRequest;
import com.acme.maintenance.v1.EstimateWorkOrderResponse;
import com.acme.maintenance.v1.WorkOrder;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexComponent;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SearchSearchIndexesRequest;
import com.acme.search.v1.SearchSearchIndexesResponse;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Moves a collateral between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class CollateralLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(CollateralLifecycleApplication.class);

    private final ProductServiceGrpc.ProductServiceBlockingStub catalogProductServiceBlocking;

    private final ChannelRegistry channels;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceFutureStub lendingCollateralValidationServiceFuture;

    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking;

    private final LoanValidationServiceGrpc.LoanValidationServiceBlockingStub lendingLoanValidationServiceBlocking;

    private final RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking;

    private final RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking;

    private final LendingRepository repository;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    public CollateralLifecycleApplication(
            ProductServiceGrpc.ProductServiceBlockingStub catalogProductServiceBlocking,
            ChannelRegistry channels,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            ApplicationEventPublisher events,
            CollateralValidationServiceGrpc.CollateralValidationServiceFutureStub lendingCollateralValidationServiceFuture,
            LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking,
            LoanValidationServiceGrpc.LoanValidationServiceBlockingStub lendingLoanValidationServiceBlocking,
            RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking,
            RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking,
            LendingRepository repository,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking) {
        this.catalogProductServiceBlocking = catalogProductServiceBlocking;
        this.channels = channels;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.events = events;
        this.lendingCollateralValidationServiceFuture = lendingCollateralValidationServiceFuture;
        this.lendingLoanApplicationValidationServiceBlocking = lendingLoanApplicationValidationServiceBlocking;
        this.lendingLoanValidationServiceBlocking = lendingLoanValidationServiceBlocking;
        this.lendingRepaymentCalculationServiceBlocking = lendingRepaymentCalculationServiceBlocking;
        this.lendingRepaymentReportingServiceBlocking = lendingRepaymentReportingServiceBlocking;
        this.repository = repository;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
    }

    public record ActivateCollateralStage0Requested(EstimateHouseholdRequest request, CompletableFuture<EstimateHouseholdResponse> completion, io.grpc.Context context) {}

    public record GetCollateralLifecycleStage0Requested(ExplainCustomerSegmentDecisionRequest request, CompletableFuture<ExplainCustomerSegmentDecisionResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a collateral.
     */
    public ActivateCollateralResponse activateCollateral(ActivateCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Collateral", request.getCollateralId(), "ActivateCollateral");

        var outbound0 = EstimateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setEmail(request.getDetails().getNewOwner().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<EstimateHouseholdResponse>();
        events.publishEvent(new ActivateCollateralStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var builder1 = ValidateHouseholdRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setHousehold(Household.newBuilder()
                .addChildren(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setCreatedAt(reply0.getHousehold().getAudit().getCreatedAt())
                                        .build())
                                .setLocation(PostalAddress.newBuilder()
                                        .addAllLines(reply0.getHousehold().getLocation().getLinesList())
                                        .setLocality(reply0.getHousehold().getLocation().getLocality())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(reply0.getHousehold().getManager().getDisplayName())
                                        .build())
                                .build())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateHousehold(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateCollateral", request.getDetails().getReasonCode());
        return ActivateCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCollateralId(request.getCollateralId())
                        .build())
                .setOutcome(ActivateCollateralResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Suspends a collateral.
     */
    public SuspendCollateralResponse suspendCollateral(SuspendCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Collateral", request.getCollateralId(), "SuspendCollateral");

        var outbound0 = SearchSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .build())
                .build();
        RpcPort<SearchSearchIndexesRequest, SearchSearchIndexesResponse> port0 = RpcPort.adapt(searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::searchSearchIndexes);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = NormalizeLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeLoanRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeLoan(outbound1);

        var outbound2 = LintLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply1.getLoan().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<LintLoanApplicationsRequest, LintLoanApplicationsResponse> port2 = RpcPort.adapt(lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintLoanApplications);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendCollateral", request.getDetails().getReasonCode());
        return SuspendCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getLoanApplication().getAudit().getEtag())
                                .build())
                        .setCollateralId(request.getCollateralId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getLoan().getContact().getEmail())
                                .build())
                        .build())
                .setOutcome(SuspendCollateralResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Reinstates a collateral.
     */
    public ReinstateCollateralResponse reinstateCollateral(ReinstateCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Collateral", request.getCollateralId(), "ReinstateCollateral");

        var outbound0 = EstimateWorkOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setWorkOrder(WorkOrder.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.maintenance.v1.WorkOrderCalculationService");
        var serviceName0 = "acme.maintenance.v1" + "." + "WorkOrderCalculationService";
        var methodName0 = "Est" + "imateWorkOrder";
        var descriptor0 = MethodDescriptor.<EstimateWorkOrderRequest, EstimateWorkOrderResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(EstimateWorkOrderRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(EstimateWorkOrderResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = BeanCopies.copy(EstimateRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getWorkOrder().getAudit().getCreatedAt())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                                        .setName(request.getDetails().getNotifyContact().getName())
                                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                                        .build())
                                .build())
                        .build())
                .build(), EstimateRepaymentRequest.class);
        var reply1 = lendingRepaymentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateRepayment(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateCollateral", request.getDetails().getReasonCode());
        return ReinstateCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCollateralId(request.getCollateralId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getWorkOrder().getCustomer().getEmail())
                                .build())
                        .build())
                .setOutcome(ReinstateCollateralResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Terminates a collateral.
     */
    public TerminateCollateralResponse terminateCollateral(TerminateCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Collateral", request.getCollateralId(), "TerminateCollateral");

        var outbound0 = CountRepaymentTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountRepaymentTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> lendingRepaymentReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countRepaymentTotals(outbound0));
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateCollateral", request.getDetails().getReasonCode());
        return TerminateCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setCollateralId(request.getCollateralId())
                        .build())
                .setOutcome(TerminateCollateralResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Renews a collateral.
     */
    public RenewCollateralResponse renewCollateral(RenewCollateralRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewCollateral")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Collateral", request.getCollateralId(), "RenewCollateral");

        var outbound0 = BeanCopies.copy(CreateProductRequest.newBuilder()
                .setContext(request.getContext())
                .setProduct(Product.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build(), CreateProductRequest.class);
        var reply0 = catalogProductServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createProduct(outbound0);

        var outbound1 = (UpdateSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .putComponents("primary", SearchIndexComponent.newBuilder()
                                .setQuantity(reply0.getProduct().getComponentsOrDefault("primary", ProductComponent.getDefaultInstance()).getQuantity())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateSearchIndex(outbound1);

        var outbound2 = ValidateRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateRepaymentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getProduct().getOwner().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.lending.v1.RepaymentValidationService");
        var serviceName2 = "acme.lending.v1" + "." + "RepaymentValidationService";
        var methodName2 = "Val" + "idateRepayment";
        var descriptor2 = MethodDescriptor.<ValidateRepaymentRequest, ValidateRepaymentResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ValidateRepaymentRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ValidateRepaymentResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = NormalizeCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getProduct().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending3 = lendingCollateralValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeCollateral(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewCollateral", request.getDetails().getReasonCode());
        return RenewCollateralResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getProduct().getAudit().getUpdatedBy())
                                .build())
                        .setCapacity(reply3.getCollateral().getCapacity())
                        .setCode(reply1.getSearchIndex().getCode())
                        .setCollateralId(request.getCollateralId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply3.getCollateral().getOwner().getDisplayName())
                                .build())
                        .build())
                .setOutcome(RenewCollateralResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a collateral.
     */
    public GetCollateralLifecycleResponse getCollateralLifecycle(GetCollateralLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCollateralLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Collateral", request.getCollateralId(), "GetCollateralLifecycle");

        var outbound0 = ExplainCustomerSegmentDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ExplainCustomerSegmentDecisionResponse>();
        events.publishEvent(new GetCollateralLifecycleStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return GetCollateralLifecycleResponse.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCustomerSegment().getAudit().getEtag())
                                .build())
                        .setCollateralId(request.getCollateralId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }
}
