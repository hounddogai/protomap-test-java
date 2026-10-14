package com.acme.finance.domains.contracts;

import com.acme.collections.v1.AdvanceWriteOffWorkflowRequest;
import com.acme.collections.v1.AdvanceWriteOffWorkflowResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ApproveContractClauseRequest;
import com.acme.contracts.v1.ApproveContractClauseResponse;
import com.acme.contracts.v1.ApproveRenewalRequest;
import com.acme.contracts.v1.ApproveRenewalResponse;
import com.acme.contracts.v1.CancelContractWorkflowRequest;
import com.acme.contracts.v1.CancelContractWorkflowResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseApprovalServiceGrpc;
import com.acme.contracts.v1.ContractClauseQueryServiceGrpc;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.CountContractClausesRequest;
import com.acme.contracts.v1.CountContractClausesResponse;
import com.acme.contracts.v1.DelegateRenewalApprovalRequest;
import com.acme.contracts.v1.DelegateRenewalApprovalResponse;
import com.acme.contracts.v1.GetRenewalApprovalStatusRequest;
import com.acme.contracts.v1.GetRenewalApprovalStatusResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.RecallRenewalRequest;
import com.acme.contracts.v1.RecallRenewalResponse;
import com.acme.contracts.v1.RejectRenewalRequest;
import com.acme.contracts.v1.RejectRenewalResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.ResolveContractsRequest;
import com.acme.contracts.v1.ResolveContractsResponse;
import com.acme.contracts.v1.SubmitRenewalRequest;
import com.acme.contracts.v1.SubmitRenewalResponse;
import com.acme.contracts.v1.ValidateObligationRequest;
import com.acme.contracts.v1.ValidateObligationResponse;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.drivers.v1.Driver;
import com.acme.drivers.v1.DriverServiceGrpc;
import com.acme.drivers.v1.UpdateDriverRequest;
import com.acme.drivers.v1.UpdateDriverResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.SearchNotificationsRequest;
import com.acme.notifications.v1.SearchNotificationsResponse;
import com.acme.risk.v1.EscalateRiskRegisterRequest;
import com.acme.risk.v1.EscalateRiskRegisterResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Routes renewals through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class RenewalApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(RenewalApprovalApplication.class);

    private final ChannelRegistry channels;

    private final WriteOffWorkflowServiceGrpc.WriteOffWorkflowServiceBlockingStub collectionsWriteOffWorkflowServiceBlocking;

    private final ContractClauseApprovalServiceGrpc.ContractClauseApprovalServiceBlockingStub contractsContractClauseApprovalServiceBlocking;

    private final ContractClauseQueryServiceGrpc.ContractClauseQueryServiceBlockingStub contractsContractClauseQueryServiceBlocking;

    private final ContractQueryServiceGrpc.ContractQueryServiceBlockingStub contractsContractQueryServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final DriverServiceGrpc.DriverServiceBlockingStub driversDriverServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final ContractsRepository repository;

    private final RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking;

    private final Executor rpcExecutor;

    public RenewalApprovalApplication(
            ChannelRegistry channels,
            WriteOffWorkflowServiceGrpc.WriteOffWorkflowServiceBlockingStub collectionsWriteOffWorkflowServiceBlocking,
            ContractClauseApprovalServiceGrpc.ContractClauseApprovalServiceBlockingStub contractsContractClauseApprovalServiceBlocking,
            ContractClauseQueryServiceGrpc.ContractClauseQueryServiceBlockingStub contractsContractClauseQueryServiceBlocking,
            ContractQueryServiceGrpc.ContractQueryServiceBlockingStub contractsContractQueryServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            DriverServiceGrpc.DriverServiceBlockingStub driversDriverServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            ContractsRepository repository,
            RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.channels = channels;
        this.collectionsWriteOffWorkflowServiceBlocking = collectionsWriteOffWorkflowServiceBlocking;
        this.contractsContractClauseApprovalServiceBlocking = contractsContractClauseApprovalServiceBlocking;
        this.contractsContractClauseQueryServiceBlocking = contractsContractClauseQueryServiceBlocking;
        this.contractsContractQueryServiceBlocking = contractsContractQueryServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.driversDriverServiceBlocking = driversDriverServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterApprovalServiceBlocking = riskRiskRegisterApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Submits a renewal.
     */
    public SubmitRenewalResponse submitRenewal(SubmitRenewalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitRenewal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var outbound0 = ResolveContractsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> contractsContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveContracts(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitRenewal", request.getRenewal().getRenewalId());
        return SubmitRenewalResponse.newBuilder()
                .setOutcome(SubmitRenewalResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .setRenewalId(request.getRenewalId())
                        .setWindow(request.getRenewal().getWindow())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves a renewal.
     */
    public ApproveRenewalResponse approveRenewal(ApproveRenewalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveRenewal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var builder0 = CompareHouseholdOptionsRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setHousehold(Household.newBuilder()
                .addChildren(Household.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getRenewal().getAudit().getUpdatedAt())
                                .build())
                        .addChildren(Household.newBuilder()
                                .setLocation(PostalAddress.newBuilder()
                                        .addAllLines(request.getRenewal().getLocation().getLinesList())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(request.getRenewal().getOrganizer().getDisplayName())
                                        .setUserId(request.getDetails().getApprover().getUserId())
                                        .build())
                                .build())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareHouseholdOptions(outbound0);

        var outbound1 = SearchNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRenewal().getAudit().getCreatedAt())
                        .setRegion(request.getRenewal().getLocation().getRegion())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchNotifications(value)).orElseThrow();

        var futures2 = request.getRenewal().getParticipantsList().stream()
                .map(element -> EscalateRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getRenewal().getAudit().getCreatedAt())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setLocality(reply0.getHousehold().getLocation().getLocality())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(element.getContact().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> riskRiskRegisterApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateRiskRegister(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? EscalateRiskRegisterResponse.getDefaultInstance()
                : results2.getLast();

        var builder3 = ApproveContractClauseRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setContractClause(ContractClause.newBuilder()
                .setOwner(UserRef.newBuilder()
                        .setEmail(reply2.getRiskRegister().getManager().getEmail())
                        .build())
                .build());
        builder3.setDetails(ApproveContractClauseRequest.Details.newBuilder()
                .setApprover(UserRef.newBuilder()
                        .setDisplayName(reply2.getRiskRegister().getManager().getDisplayName())
                        .build())
                .build());
        var outbound3 = builder3.build();
        var reply3 = contractsContractClauseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveContractClause(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveRenewal", request.getRenewal().getRenewalId());
        return ApproveRenewalResponse.newBuilder()
                .setRenewal(Renewal.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply0.getHousehold().getLocation().getLinesList())
                                .build())
                        .putAllMetadata(request.getRenewal().getMetadataMap())
                        .setRenewalId(request.getRenewalId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects a renewal.
     */
    public RejectRenewalResponse rejectRenewal(RejectRenewalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectRenewal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var outbound0 = ValidateObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getRenewal().getOrganizer().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.contracts.v1.ObligationValidationService");
        var selected0 = ObligationValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.validateObligation(outbound0);

        var outbound1 = JsonCodec.roundTrip(ApproveContractClauseRequest.newBuilder()
                .setContext(request.getContext())
                .setContractClause(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRenewal().getAudit().getCreatedAt())
                                .setEtag(request.getRenewal().getAudit().getEtag())
                                .setUpdatedAt(reply0.getObligation().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getObligation().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), ApproveContractClauseRequest.class);
        var reply1 = contractsContractClauseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveContractClause(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectRenewal", request.getRenewal().getRenewalId());
        return RejectRenewalResponse.newBuilder()
                .setOutcome(RejectRenewalResponse.Outcome.newBuilder()
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getObligation().getAudit().getCreatedBy())
                                .build())
                        .setRenewalId(request.getRenewalId())
                        .setWindow(request.getRenewal().getWindow())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Delegates a renewal.
     */
    public DelegateRenewalApprovalResponse delegateRenewalApproval(DelegateRenewalApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateRenewalApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var builder0 = AdvanceWriteOffWorkflowRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setWriteOff(WriteOff.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedBy(request.getRenewal().getAudit().getUpdatedBy())
                        .build())
                .setBillingAddress(PostalAddress.newBuilder()
                        .setRegion(request.getRenewal().getLocation().getRegion())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = collectionsWriteOffWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceWriteOffWorkflow(outbound0);

        var outbound1 = ResolveCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var adapter1 = new Function<ResolveCustomersRequest, ResolveCustomersResponse>() {
            @Override
            public ResolveCustomersResponse apply(ResolveCustomersRequest input) {
                return customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveCustomers(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateRenewalApproval", request.getRenewal().getRenewalId());
        return DelegateRenewalApprovalResponse.newBuilder()
                .setOutcome(DelegateRenewalApprovalResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .setRenewalId(request.getRenewalId())
                        .setStatus(request.getRenewal().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalls a renewal.
     */
    public RecallRenewalResponse recallRenewal(RecallRenewalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallRenewal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var outbound0 = CountContractClausesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountContractClausesRequest.Filter.newBuilder()
                        .setRegion(request.getRenewal().getLocation().getRegion())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> contractsContractClauseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countContractClauses(outbound0));

        var outbound1 = UpdateDriverRequest.newBuilder()
                .setContext(request.getContext())
                .setDriver(Driver.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getRenewal().getOrganizer().getEmail())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = driversDriverServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateDriver(outbound1);

        var outbound2 = CancelContractWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setContract(Contract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getRenewal().getLocation().getLinesList())
                                .setRecipient(request.getRenewal().getLocation().getRecipient())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.contracts.v1.ContractWorkflowService");
        var serviceName2 = "acme.contracts.v1" + "." + "ContractWorkflowService";
        var methodName2 = "Can" + "celContractWorkflow";
        var descriptor2 = MethodDescriptor.<CancelContractWorkflowRequest, CancelContractWorkflowResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(CancelContractWorkflowRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CancelContractWorkflowResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallRenewal", request.getRenewal().getRenewalId());
        return RecallRenewalResponse.newBuilder()
                .setOutcome(RecallRenewalResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getDriver().getPrimaryAddress().getPostalCode())
                                .setRecipient(reply2.getContract().getBillingAddress().getRecipient())
                                .build())
                        .setRenewalId(request.getRenewalId())
                        .setStatus(request.getRenewal().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a renewal.
     */
    public GetRenewalApprovalStatusResponse getRenewalApprovalStatus(GetRenewalApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRenewalApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Renewal", request.getRenewalId(), "GetRenewalApprovalStatus");
        return GetRenewalApprovalStatusResponse.newBuilder()
                .setRenewal(Renewal.newBuilder()
                        .setRenewalId(request.getRenewalId())
                        .build())
                .build();
    }
}
