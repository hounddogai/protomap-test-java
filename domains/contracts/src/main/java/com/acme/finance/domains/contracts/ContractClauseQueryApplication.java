package com.acme.finance.domains.contracts;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.NormalizeAssetTransferRequest;
import com.acme.assets.v1.NormalizeAssetTransferResponse;
import com.acme.audit.v1.SummarizeAuditTrailsRequest;
import com.acme.audit.v1.SummarizeAuditTrailsResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.ExportBankAccountsRequest;
import com.acme.banking.v1.ExportBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BrowseContractClausesRequest;
import com.acme.contracts.v1.BrowseContractClausesResponse;
import com.acme.contracts.v1.CheckObligationEligibilityRequest;
import com.acme.contracts.v1.CheckObligationEligibilityResponse;
import com.acme.contracts.v1.CheckRenewalEligibilityRequest;
import com.acme.contracts.v1.CheckRenewalEligibilityResponse;
import com.acme.contracts.v1.CompareObligationOptionsRequest;
import com.acme.contracts.v1.CompareObligationOptionsResponse;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.CountContractClausesRequest;
import com.acme.contracts.v1.CountContractClausesResponse;
import com.acme.contracts.v1.ExportContractClausesRequest;
import com.acme.contracts.v1.ExportContractClausesResponse;
import com.acme.contracts.v1.FindContractClausesRequest;
import com.acme.contracts.v1.FindContractClausesResponse;
import com.acme.contracts.v1.FindContractsRequest;
import com.acme.contracts.v1.FindContractsResponse;
import com.acme.contracts.v1.LookupContractClauseRequest;
import com.acme.contracts.v1.LookupContractClauseResponse;
import com.acme.contracts.v1.NormalizeRenewalRequest;
import com.acme.contracts.v1.NormalizeRenewalResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.PeekContractClauseRequest;
import com.acme.contracts.v1.PeekContractClauseResponse;
import com.acme.contracts.v1.PreviewRenewalRequest;
import com.acme.contracts.v1.PreviewRenewalResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalDailyRule;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ResolveContractClausesRequest;
import com.acme.contracts.v1.ResolveContractClausesResponse;
import com.acme.contracts.v1.ScoreRenewalRequest;
import com.acme.contracts.v1.ScoreRenewalResponse;
import com.acme.contracts.v1.SummarizeContractClausesRequest;
import com.acme.contracts.v1.SummarizeContractClausesResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.ResolvePoliciesRequest;
import com.acme.insurance.v1.ResolvePoliciesResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about contract clauses, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ContractClauseQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(ContractClauseQueryApplication.class);

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final ContractQueryServiceGrpc.ContractQueryServiceBlockingStub contractsContractQueryServiceBlocking;

    private final ObligationCalculationServiceGrpc.ObligationCalculationServiceBlockingStub contractsObligationCalculationServiceBlocking;

    private final ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceFutureStub contractsRenewalValidationServiceFuture;

    private final PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking;

    private final ContractClauseQueryRequestMapper mapper;

    private final ContractsRepository repository;

    public ContractClauseQueryApplication(
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking,
            ChannelRegistry channels,
            ContractQueryServiceGrpc.ContractQueryServiceBlockingStub contractsContractQueryServiceBlocking,
            ObligationCalculationServiceGrpc.ObligationCalculationServiceBlockingStub contractsObligationCalculationServiceBlocking,
            ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking,
            RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking,
            RenewalValidationServiceGrpc.RenewalValidationServiceFutureStub contractsRenewalValidationServiceFuture,
            PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking,
            ContractClauseQueryRequestMapper mapper,
            ContractsRepository repository) {
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.bankingBankAccountQueryServiceBlocking = bankingBankAccountQueryServiceBlocking;
        this.channels = channels;
        this.contractsContractQueryServiceBlocking = contractsContractQueryServiceBlocking;
        this.contractsObligationCalculationServiceBlocking = contractsObligationCalculationServiceBlocking;
        this.contractsObligationValidationServiceBlocking = contractsObligationValidationServiceBlocking;
        this.contractsRenewalValidationServiceBlocking = contractsRenewalValidationServiceBlocking;
        this.contractsRenewalValidationServiceFuture = contractsRenewalValidationServiceFuture;
        this.insurancePolicyQueryServiceBlocking = insurancePolicyQueryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
    }

    /**
     * Returns a contract clause.
     */
    public LookupContractClauseResponse lookupContractClause(LookupContractClauseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupContractClause")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ContractClause", request.getContractClauseId(), "LookupContractClause");

        var outbound0 = RpcSupport.unpack(Any.pack(CheckObligationEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckObligationEligibilityRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build()), CheckObligationEligibilityRequest.class);
        var reply0 = contractsObligationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkObligationEligibility(outbound0);
        return LookupContractClauseResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setContractClauseId(request.getContractClauseId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getObligation().getCustomer().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists contract clauses a page at a time.
     */
    public BrowseContractClausesResponse browseContractClauses(BrowseContractClausesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseContractClauses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ContractClause", request.getContext().getTraceId(), "BrowseContractClauses");

        var outbound0 = PreviewRenewalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewRenewalRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.contracts.v1.RenewalValidationService/PreviewRenewal", outbound0);
        var reply0 = contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewRenewal(command0.payload());

        var outbound1 = CheckRenewalEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRenewal().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getRenewal().getAudit().getUpdatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getRenewal().getLocation().getCountryCode())
                                .addAllLines(reply0.getRenewal().getLocation().getLinesList())
                                .build())
                        .build())
                .build();
        RpcPort<CheckRenewalEligibilityRequest, CheckRenewalEligibilityResponse> port1 = RpcPort.adapt(contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::checkRenewalEligibility);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseContractClauses", request.getPage().getPageToken());
        return BrowseContractClausesResponse.newBuilder()
                .addContractClauses(ContractClause.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply1.getRenewal().getOrganizer().getUserId())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists contract clauses a page at a time.
     */
    public FindContractClausesResponse findContractClauses(FindContractClausesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindContractClauses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ContractClause", request.getContext().getTraceId(), "FindContractClauses");

        var outbound0 = mapper.findContractClausesStage0(request);
        var reply0 = assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAssetTransfer(outbound0);

        var outbound1 = SummarizeAuditTrailsRequest.newBuilder()
                .putAllAnnotations(request.getFilter().getLabelsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeAuditTrailsRequest.Details.newBuilder()
                        .putAllFilters(reply0.getResult().getDetailsMap())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.audit.v1.AuditTrailQueryService");
        var serviceName1 = "acme.audit.v1" + "." + "AuditTrailQueryService";
        var methodName1 = "Sum" + "marizeAuditTrails";
        var descriptor1 = MethodDescriptor.<SummarizeAuditTrailsRequest, SummarizeAuditTrailsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(SummarizeAuditTrailsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SummarizeAuditTrailsResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindContractClauses", request.getPage().getPageToken());
        return FindContractClausesResponse.newBuilder()
                .addContractClauses(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getAssetTransfer().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Counts contract clauses by status.
     */
    public CountContractClausesResponse countContractClauses(CountContractClausesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountContractClauses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ContractClause", request.getContext().getTraceId(), "CountContractClauses");

        var outbound0 = JsonCodec.roundTrip(CheckRenewalEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckRenewalEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), CheckRenewalEligibilityRequest.class);
        var reply0 = contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkRenewalEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountContractClauses", request.getPage().getPageToken());
        return CountContractClausesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several contract clauses at once.
     */
    public ResolveContractClausesResponse resolveContractClauses(ResolveContractClausesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveContractClauses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ContractClause", request.getContext().getTraceId(), "ResolveContractClauses");

        var outbound0 = RpcSupport.unpack(Any.pack(NormalizeRenewalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeRenewalRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build()), NormalizeRenewalRequest.class);
        var reply0 = contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRenewal(outbound0);
        return ResolveContractClausesResponse.newBuilder()
                .addContractClauses(ContractClause.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRenewal().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
    }

    /**
     * Streams every matching contract clause.
     */
    public ExportContractClausesResponse exportContractClauses(ExportContractClausesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportContractClauses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ContractClause", request.getContext().getTraceId(), "ExportContractClauses");

        var builder0 = FindContractsRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setFilter(FindContractsRequest.Filter.newBuilder()
                .setCreatedAfter(request.getFilter().getCreatedAfter())
                .putAllLabels(request.getFilter().getLabelsMap())
                .setOwnerId(request.getFilter().getOwnerId())
                .setRegion(request.getFilter().getRegion())
                .build());
        var outbound0 = builder0.build();
        var reply0 = contractsContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findContracts(outbound0);

        var outbound1 = ScoreRenewalRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRenewal(outbound1);

        var outbound2 = PreviewRenewalRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getRenewal().getAudit().getEtag())
                                .build())
                        .setDaily(RenewalDailyRule.newBuilder()
                                .setAtHour(reply1.getRenewal().getDaily().getAtHour())
                                .build())
                        .build())
                .build();
        var reply2 = contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewRenewal(outbound2);

        var outbound3 = RpcSupport.unpack(Any.pack(CompareObligationOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareObligationOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply1.getRenewal().getLocation().getLocality())
                                .build())
                        .build())
                .setObligation(Obligation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getRenewal().getAudit().getEtag())
                                .build())
                        .build())
                .build()), CompareObligationOptionsRequest.class);
        var reply3 = contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareObligationOptions(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportContractClauses", request.getFilter().getQuery());
        return ExportContractClausesResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getRenewal().getOrganizer().getEmail())
                                .build())
                        .setPriority(reply3.getObligation().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Summarizes a contract clause.
     */
    public SummarizeContractClausesResponse summarizeContractClauses(SummarizeContractClausesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeContractClauses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ContractClause", request.getContractClauseId(), "SummarizeContractClauses");

        var outbound0 = CheckRenewalEligibilityRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(CheckRenewalEligibilityRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setReason(request.getReason())
                .setRenewal(Renewal.newBuilder()
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending0 = contractsRenewalValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).checkRenewalEligibility(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = ExportBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportBankAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getOutcome().getEffectiveAt())
                        .setOwnerId(reply0.getRenewal().getOrganizer().getUserId())
                        .setRegion(reply0.getRenewal().getLocation().getRegion())
                        .build())
                .setFormat(request.getDetails().getFormat())
                .build();
        var stream1 = bankingBankAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportBankAccounts(outbound1);
        var reply1 = ExportBankAccountsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getContractClauseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeContractClauses", request.getContractClauseId());
        return SummarizeContractClausesResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a contract clause.
     */
    public PeekContractClauseResponse peekContractClause(PeekContractClauseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekContractClause")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ContractClause", request.getContractClauseId(), "PeekContractClause");

        var outbound0 = ResolvePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.retryRead(() -> insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolvePolicies(outbound0));
        return PeekContractClauseResponse.newBuilder()
                .setContractClause(ContractClause.newBuilder()
                        .setContractClauseId(request.getContractClauseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }
}
