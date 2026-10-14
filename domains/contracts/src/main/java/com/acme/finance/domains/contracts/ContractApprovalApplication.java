package com.acme.finance.domains.contracts;

import com.acme.assets.v1.ApproveFixedAssetRequest;
import com.acme.assets.v1.ApproveFixedAssetResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferApprovalServiceGrpc;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.RejectAssetTransferRequest;
import com.acme.assets.v1.RejectAssetTransferResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.SubmitStandingOrderRequest;
import com.acme.banking.v1.SubmitStandingOrderResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseWorkflowServiceGrpc;
import com.acme.compliance.v1.ListSanctionReportsRequest;
import com.acme.compliance.v1.ListSanctionReportsResponse;
import com.acme.compliance.v1.RecallSanctionRequest;
import com.acme.compliance.v1.RecallSanctionResponse;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionApprovalServiceGrpc;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.contracts.v1.ApproveContractRequest;
import com.acme.contracts.v1.ApproveContractResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClauseQueryServiceGrpc;
import com.acme.contracts.v1.ContractLine;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.DelegateContractApprovalRequest;
import com.acme.contracts.v1.DelegateContractApprovalResponse;
import com.acme.contracts.v1.EscalateContractRequest;
import com.acme.contracts.v1.EscalateContractResponse;
import com.acme.contracts.v1.ExportContractsRequest;
import com.acme.contracts.v1.ExportContractsResponse;
import com.acme.contracts.v1.GetContractApprovalStatusRequest;
import com.acme.contracts.v1.GetContractApprovalStatusResponse;
import com.acme.contracts.v1.GetRenewalRulesRequest;
import com.acme.contracts.v1.GetRenewalRulesResponse;
import com.acme.contracts.v1.LintObligationsRequest;
import com.acme.contracts.v1.LintObligationsResponse;
import com.acme.contracts.v1.ListPendingContractsRequest;
import com.acme.contracts.v1.ListPendingContractsResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationSla;
import com.acme.contracts.v1.RecallContractRequest;
import com.acme.contracts.v1.RecallContractResponse;
import com.acme.contracts.v1.RejectContractRequest;
import com.acme.contracts.v1.RejectContractResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalRef;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ResolveContractClausesRequest;
import com.acme.contracts.v1.ResolveContractClausesResponse;
import com.acme.contracts.v1.ScoreRenewalRequest;
import com.acme.contracts.v1.ScoreRenewalResponse;
import com.acme.contracts.v1.SubmitContractRequest;
import com.acme.contracts.v1.SubmitContractResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.NormalizePreferenceRequest;
import com.acme.customers.v1.NormalizePreferenceResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.identity.v1.ValidateUserRequest;
import com.acme.identity.v1.ValidateUserResponse;
import com.acme.media.v1.BatchGetMediaAssetsRequest;
import com.acme.media.v1.BatchGetMediaAssetsResponse;
import com.acme.media.v1.MediaAssetServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Routes contracts through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ContractApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(ContractApprovalApplication.class);

    private final AssetTransferApprovalServiceGrpc.AssetTransferApprovalServiceBlockingStub assetsAssetTransferApprovalServiceBlocking;

    private final FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking;

    private final StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseWorkflowServiceGrpc.ComplianceCaseWorkflowServiceBlockingStub complianceComplianceCaseWorkflowServiceBlocking;

    private final SanctionApprovalServiceGrpc.SanctionApprovalServiceBlockingStub complianceSanctionApprovalServiceBlocking;

    private final SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking;

    private final ContractClauseQueryServiceGrpc.ContractClauseQueryServiceBlockingStub contractsContractClauseQueryServiceBlocking;

    private final ContractQueryServiceGrpc.ContractQueryServiceBlockingStub contractsContractQueryServiceBlocking;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final ContractApprovalRequestMapper mapper;

    private final MediaAssetServiceGrpc.MediaAssetServiceBlockingStub mediaMediaAssetServiceBlocking;

    private final ContractsRepository repository;

    private final Executor rpcExecutor;

    public ContractApprovalApplication(
            AssetTransferApprovalServiceGrpc.AssetTransferApprovalServiceBlockingStub assetsAssetTransferApprovalServiceBlocking,
            FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking,
            StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseWorkflowServiceGrpc.ComplianceCaseWorkflowServiceBlockingStub complianceComplianceCaseWorkflowServiceBlocking,
            SanctionApprovalServiceGrpc.SanctionApprovalServiceBlockingStub complianceSanctionApprovalServiceBlocking,
            SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking,
            ContractClauseQueryServiceGrpc.ContractClauseQueryServiceBlockingStub contractsContractClauseQueryServiceBlocking,
            ContractQueryServiceGrpc.ContractQueryServiceBlockingStub contractsContractQueryServiceBlocking,
            RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            ContractApprovalRequestMapper mapper,
            MediaAssetServiceGrpc.MediaAssetServiceBlockingStub mediaMediaAssetServiceBlocking,
            ContractsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsAssetTransferApprovalServiceBlocking = assetsAssetTransferApprovalServiceBlocking;
        this.assetsFixedAssetApprovalServiceBlocking = assetsFixedAssetApprovalServiceBlocking;
        this.bankingStandingOrderApprovalServiceBlocking = bankingStandingOrderApprovalServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseWorkflowServiceBlocking = complianceComplianceCaseWorkflowServiceBlocking;
        this.complianceSanctionApprovalServiceBlocking = complianceSanctionApprovalServiceBlocking;
        this.complianceSanctionReportingServiceBlocking = complianceSanctionReportingServiceBlocking;
        this.contractsContractClauseQueryServiceBlocking = contractsContractClauseQueryServiceBlocking;
        this.contractsContractQueryServiceBlocking = contractsContractQueryServiceBlocking;
        this.contractsRenewalValidationServiceBlocking = contractsRenewalValidationServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.mapper = mapper;
        this.mediaMediaAssetServiceBlocking = mediaMediaAssetServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Submits a contract.
     */
    public SubmitContractResponse submitContract(SubmitContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var builder0 = ApproveFixedAssetRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setFixedAsset(FixedAsset.newBuilder()
                .setAddress(PostalAddress.newBuilder()
                        .setPostalCode(request.getContract().getBillingAddress().getPostalCode())
                        .setRecipient(request.getContract().getBillingAddress().getRecipient())
                        .setRegion(request.getContract().getBillingAddress().getRegion())
                        .build())
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedBy(request.getContract().getAudit().getUpdatedBy())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = assetsFixedAssetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveFixedAsset(outbound0);

        var outbound1 = RecallSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getContract().getAudit().getCreatedAt())
                                                .setEtag(request.getContract().getAudit().getEtag())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getApprover().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.compliance.v1.SanctionApprovalService/RecallSanction", outbound1);
        var reply1 = complianceSanctionApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallSanction(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getContract().getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitContract", request.getContract().getContractId());
        return SubmitContractResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getSanction().getAudit().getUpdatedAt())
                                .build())
                        .setContractId(request.getContractId())
                        .setCounterparty(RenewalRef.newBuilder()
                                .setDisplayName(reply0.getFixedAsset().getOwner().getDisplayName())
                                .build())
                        .setIssuedAt(request.getContract().getIssuedAt())
                        .build())
                .build();
    }

    /**
     * Approves a contract.
     */
    public ApproveContractResponse approveContract(ApproveContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = ValidateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getContract().getAudit().getEtag())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRegion(request.getContract().getBillingAddress().getRegion())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(request.getContract().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateUser(outbound0), rpcExecutor);

        var outbound1 = SubmitStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContract().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> bankingStandingOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitStandingOrder(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getContract().getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveContract", request.getContract().getContractId());
        return ApproveContractResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setContractId(request.getContractId())
                        .setNumber(reply0.getUser().getNationalId().getNumber())
                        .build())
                .setOutcome(ApproveContractResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects a contract.
     */
    public RejectContractResponse rejectContract(RejectContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = LintObligationsRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .setSla(ObligationSla.newBuilder()
                                .setDueAt(request.getContract().getDueAt())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.contracts.v1.ObligationValidationService");
        var serviceName0 = "acme.contracts.v1" + "." + "ObligationValidationService";
        var methodName0 = "Lin" + "tObligations";
        var descriptor0 = MethodDescriptor.<LintObligationsRequest, LintObligationsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(LintObligationsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(LintObligationsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getContract().getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectContract", request.getContract().getContractId());
        return RejectContractResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setContractId(request.getContractId())
                        .putAllMetadata(request.getContract().getMetadataMap())
                        .build())
                .setOutcome(RejectContractResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Escalates a contract.
     */
    public EscalateContractResponse escalateContract(EscalateContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = BatchGetMediaAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var command0 = new DispatchCommand<>("acme.media.v1.MediaAssetService/BatchGetMediaAssets", outbound0);
        var reply0 = mediaMediaAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetMediaAssets(command0.payload());

        var outbound1 = GetRenewalRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewalId(request.getContract().getCounterparty().getRenewalId())
                .build();
        var command1 = new DispatchCommand<>("acme.contracts.v1.RenewalValidationService/GetRenewalRules", outbound1);
        var reply1 = contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getRenewalRules(command1.payload());

        var outbound2 = RpcSupport.unpack(Any.pack(NormalizePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContract().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getRenewal().getOrganizer().getDisplayName())
                                .setEmail(reply1.getRenewal().getOrganizer().getEmail())
                                .build())
                        .build())
                .build()), NormalizePreferenceRequest.class);
        var reply2 = customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePreference(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getContract().getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateContract", request.getContract().getContractId());
        return EscalateContractResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getRenewal().getAudit().getEtag())
                                .build())
                        .setContractId(request.getContractId())
                        .setCounterparty(RenewalRef.newBuilder()
                                .setRenewalId(reply1.getRenewal().getRenewalId())
                                .build())
                        .setDueAt(request.getContract().getDueAt())
                        .addLines(ContractLine.newBuilder()
                                .setDescription(reply2.getPreference().getDescription())
                                .build())
                        .build())
                .setOutcome(EscalateContractResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists contracts a page at a time.
     */
    public ListPendingContractsResponse listPendingContracts(ListPendingContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContext().getTraceId(), "ListPendingContracts");

        var outbound0 = ResolveContractClausesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> contractsContractClauseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveContractClauses(value));
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingContracts", request.getPage().getPageToken());
        return ListPendingContractsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Delegates a contract.
     */
    public DelegateContractApprovalResponse delegateContractApproval(DelegateContractApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateContractApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = mapper.delegateContractApprovalStage0(request);
        var reply0 = assetsAssetTransferApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectAssetTransfer(outbound0);

        var outbound1 = ExportContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContract().getAudit().getUpdatedAt())
                        .setRegion(request.getContract().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = contractsContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportContracts(outbound1);
        var reply1 = ExportContractsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var builder2 = ScoreRenewalRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setDetails(ScoreRenewalRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .setPostalCode(reply0.getAssetTransfer().getBillingAddress().getPostalCode())
                        .build())
                .build());
        builder2.setRenewal(Renewal.newBuilder()
                .setLocation(PostalAddress.newBuilder()
                        .setCountryCode(reply0.getAssetTransfer().getBillingAddress().getCountryCode())
                        .addAllLines(request.getContract().getBillingAddress().getLinesList())
                        .build())
                .build());
        var outbound2 = builder2.build();
        var reply2 = contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRenewal(outbound2);

        var outbound3 = (ListSanctionReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListSanctionReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply2.getRenewal().getAudit().getUpdatedAt())
                        .setOwnerId(reply0.getAssetTransfer().getCounterparty().getFixedAssetId())
                        .setRegion(reply2.getRenewal().getLocation().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply3 = complianceSanctionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listSanctionReports(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getContract().getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateContractApproval", request.getContract().getContractId());
        return DelegateContractApprovalResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getAssetTransfer().getBillingAddress().getPostalCode())
                                .build())
                        .setContractId(request.getContractId())
                        .setDueAt(request.getContract().getDueAt())
                        .build())
                .setOutcome(DelegateContractApprovalResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalls a contract.
     */
    public RecallContractResponse recallContract(RecallContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveContract(request.getContext().getTenantId(), request.getContract());

        var outbound0 = (ResumeComplianceCaseWorkflowRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getContract().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContract().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply0 = complianceComplianceCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeComplianceCaseWorkflow(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getContract().getContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallContract", request.getContract().getContractId());
        return RecallContractResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getComplianceCase().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(request.getContract().getBillingAddress())
                        .setContractId(request.getContractId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a contract.
     */
    public GetContractApprovalStatusResponse getContractApprovalStatus(GetContractApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetContractApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Contract", request.getContractId(), "GetContractApprovalStatus");
        return GetContractApprovalStatusResponse.newBuilder()
                .setContract(Contract.newBuilder()
                        .setContractId(request.getContractId())
                        .build())
                .build();
    }
}
