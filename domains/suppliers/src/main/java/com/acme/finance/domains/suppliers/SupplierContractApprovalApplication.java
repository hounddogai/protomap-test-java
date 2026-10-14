package com.acme.finance.domains.suppliers;

import com.acme.assets.v1.ApproveFixedAssetRequest;
import com.acme.assets.v1.ApproveFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.FindAccessPoliciesRequest;
import com.acme.authorization.v1.FindAccessPoliciesResponse;
import com.acme.authorization.v1.ResolveAccessPoliciesRequest;
import com.acme.authorization.v1.ResolveAccessPoliciesResponse;
import com.acme.banking.v1.ApproveMandateRequest;
import com.acme.banking.v1.ApproveMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.ShareForecastReportRequest;
import com.acme.budgeting.v1.ShareForecastReportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.ScheduleAttestationReportRequest;
import com.acme.compliance.v1.ScheduleAttestationReportResponse;
import com.acme.contracts.v1.PauseRenewalWorkflowRequest;
import com.acme.contracts.v1.PauseRenewalWorkflowResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalWorkflowServiceGrpc;
import com.acme.contracts.v1.ResumeRenewalWorkflowRequest;
import com.acme.contracts.v1.ResumeRenewalWorkflowResponse;
import com.acme.customers.v1.CompareCustomerSegmentOptionsRequest;
import com.acme.customers.v1.CompareCustomerSegmentOptionsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.drivers.v1.CreateDriverRequest;
import com.acme.drivers.v1.CreateDriverResponse;
import com.acme.drivers.v1.Driver;
import com.acme.drivers.v1.DriverServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.risk.v1.ListPendingRiskRegistersRequest;
import com.acme.risk.v1.ListPendingRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.suppliers.v1.ApproveSupplierContractRequest;
import com.acme.suppliers.v1.ApproveSupplierContractResponse;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityResponse;
import com.acme.suppliers.v1.CommentSupplierContractApprovalRequest;
import com.acme.suppliers.v1.CommentSupplierContractApprovalResponse;
import com.acme.suppliers.v1.DelegateSupplierContractApprovalRequest;
import com.acme.suppliers.v1.DelegateSupplierContractApprovalResponse;
import com.acme.suppliers.v1.DeleteSupplierRequest;
import com.acme.suppliers.v1.DeleteSupplierResponse;
import com.acme.suppliers.v1.EscalateSupplierContractRequest;
import com.acme.suppliers.v1.EscalateSupplierContractResponse;
import com.acme.suppliers.v1.GetSupplierContractApprovalStatusRequest;
import com.acme.suppliers.v1.GetSupplierContractApprovalStatusResponse;
import com.acme.suppliers.v1.ListPendingSupplierContractsRequest;
import com.acme.suppliers.v1.ListPendingSupplierContractsResponse;
import com.acme.suppliers.v1.LookupSupplierRequest;
import com.acme.suppliers.v1.LookupSupplierResponse;
import com.acme.suppliers.v1.PeekSupplierContractRequest;
import com.acme.suppliers.v1.PeekSupplierContractResponse;
import com.acme.suppliers.v1.RecallSupplierContractRequest;
import com.acme.suppliers.v1.RecallSupplierContractResponse;
import com.acme.suppliers.v1.RejectSupplierContractRequest;
import com.acme.suppliers.v1.RejectSupplierContractResponse;
import com.acme.suppliers.v1.ScoreSupplierRatingRequest;
import com.acme.suppliers.v1.ScoreSupplierRatingResponse;
import com.acme.suppliers.v1.SubmitSupplierContractRequest;
import com.acme.suppliers.v1.SubmitSupplierContractResponse;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingBankSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes supplier contracts through approval, with escalation and delegation. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class SupplierContractApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(SupplierContractApprovalApplication.class);

    private final FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking;

    private final AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking;

    private final ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking;

    private final RenewalWorkflowServiceGrpc.RenewalWorkflowServiceBlockingStub contractsRenewalWorkflowServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final DriverServiceGrpc.DriverServiceBlockingStub driversDriverServiceBlocking;

    private final ApplicationEventPublisher events;

    private final SuppliersRepository repository;

    private final RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceFutureStub suppliersSupplierRatingValidationServiceFuture;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public SupplierContractApprovalApplication(
            FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking,
            AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking,
            ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking,
            ChannelRegistry channels,
            AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking,
            RenewalWorkflowServiceGrpc.RenewalWorkflowServiceBlockingStub contractsRenewalWorkflowServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            DriverServiceGrpc.DriverServiceBlockingStub driversDriverServiceBlocking,
            ApplicationEventPublisher events,
            SuppliersRepository repository,
            RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceFutureStub suppliersSupplierRatingValidationServiceFuture,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.assetsFixedAssetApprovalServiceBlocking = assetsFixedAssetApprovalServiceBlocking;
        this.auditAuditTrailHistoryServiceBlocking = auditAuditTrailHistoryServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.bankingMandateApprovalServiceBlocking = bankingMandateApprovalServiceBlocking;
        this.budgetingForecastReportingServiceBlocking = budgetingForecastReportingServiceBlocking;
        this.channels = channels;
        this.complianceAttestationReportingServiceBlocking = complianceAttestationReportingServiceBlocking;
        this.contractsRenewalWorkflowServiceBlocking = contractsRenewalWorkflowServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.driversDriverServiceBlocking = driversDriverServiceBlocking;
        this.events = events;
        this.repository = repository;
        this.riskRiskRegisterApprovalServiceBlocking = riskRiskRegisterApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.suppliersSupplierRatingValidationServiceFuture = suppliersSupplierRatingValidationServiceFuture;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    public record GetSupplierContractApprovalStatusStage0Requested(ListMetricSeriesesRequest request, CompletableFuture<ListMetricSeriesesResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a supplier contract.
     */
    public SubmitSupplierContractResponse submitSupplierContract(SubmitSupplierContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitSupplierContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var outbound0 = ListPendingRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingRiskRegistersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplierContract().getAudit().getCreatedAt())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .setRegion(request.getSupplierContract().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> riskRiskRegisterApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingRiskRegisters(outbound0), rpcExecutor);

        var outbound1 = FindAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplierContract().getAudit().getUpdatedAt())
                        .putAllLabels(request.getSupplierContract().getMetadataMap())
                        .setOwnerId(request.getSupplierContract().getCounterparty().getSupplierId())
                        .setRegion(request.getSupplierContract().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findAccessPolicies(outbound1), rpcExecutor);

        var outbound2 = ShareForecastReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ShareForecastReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> budgetingForecastReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareForecastReport(outbound2), rpcExecutor);

        var outbound3 = ValidateSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBank(SupplierRatingBankSettlement.newBuilder()
                                .setAccountHolder(request.getSupplierContract().getBank().getAccountHolder())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getSupplierContract().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var pending3 = CompletableFuture.supplyAsync(() -> suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSupplierRating(outbound3), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2, pending3).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        var reply3 = pending3.join();
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitSupplierContract", request.getSupplierContract().getSupplierContractId());
        return SubmitSupplierContractResponse.newBuilder()
                .setOutcome(SubmitSupplierContractResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply2.getForecast().getBillingAddress().getRecipient())
                                .build())
                        .setCounterparty(request.getSupplierContract().getCounterparty())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Approves a supplier contract.
     */
    public ApproveSupplierContractResponse approveSupplierContract(ApproveSupplierContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveSupplierContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var outbound0 = JsonCodec.roundTrip(LookupSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierId(request.getSupplierContract().getCounterparty().getSupplierId())
                .build(), LookupSupplierRequest.class);
        var reply0 = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lookupSupplier(outbound0);

        var outbound1 = CompareCustomerSegmentOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getSupplierContract().getAudit().getCreatedBy())
                                .setEtag(request.getSupplierContract().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getSupplier().getRelationshipManager().getDisplayName())
                                .build())
                        .build())
                .setDetails(CompareCustomerSegmentOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(reply0.getSupplier().getPrimaryAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply1 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCustomerSegmentOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveSupplierContract", request.getSupplierContract().getSupplierContractId());
        return ApproveSupplierContractResponse.newBuilder()
                .setOutcome(ApproveSupplierContractResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getSupplier().getPrimaryAddress().getLinesList())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getSupplier().getContact().getEmail())
                                .build())
                        .setNotes(request.getSupplierContract().getNotes())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Rejects a supplier contract.
     */
    public RejectSupplierContractResponse rejectSupplierContract(RejectSupplierContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectSupplierContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var outbound0 = ScheduleAttestationReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> complianceAttestationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleAttestationReport(value)).orElseThrow();

        var outbound1 = ResumeRenewalWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getSupplierContract().getAudit().getCreatedAt())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getSupplierContract().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        RpcPort<ResumeRenewalWorkflowRequest, ResumeRenewalWorkflowResponse> port1 = RpcPort.adapt(contractsRenewalWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::resumeRenewalWorkflow);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = ResolveAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.authorization.v1.AccessPolicyQueryService");
        var selected2 = AccessPolicyQueryServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.resolveAccessPolicies(outbound2);

        var outbound3 = ScoreSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreSupplierRatingRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getSupplierContract().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getSupplierContract().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getSupplierContract().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var pending3 = suppliersSupplierRatingValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).scoreSupplierRating(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectSupplierContract", request.getSupplierContract().getSupplierContractId());
        return RejectSupplierContractResponse.newBuilder()
                .setOutcome(RejectSupplierContractResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply1.getRenewal().getOrganizer().getDisplayName())
                                .build())
                        .setNotes(request.getSupplierContract().getNotes())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Escalates a supplier contract.
     */
    public EscalateSupplierContractResponse escalateSupplierContract(EscalateSupplierContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateSupplierContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var outbound0 = JsonCodec.roundTrip(CreateDriverRequest.newBuilder()
                .setContext(request.getContext())
                .setDriver(Driver.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSupplierContract().getCounterparty().getEmail())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getSupplierContract().getBillingAddress().getCountryCode())
                                .addAllLines(request.getSupplierContract().getBillingAddress().getLinesList())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build(), CreateDriverRequest.class);
        var reply0 = driversDriverServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createDriver(outbound0);

        var outbound1 = CheckSupplierRatingEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckSupplierRatingEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getDriver().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkSupplierRatingEligibility(value));
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateSupplierContract", request.getSupplierContract().getSupplierContractId());
        return EscalateSupplierContractResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getDriver().getPrimaryAddress().getLocality())
                                .build())
                        .putAllMetadata(request.getSupplierContract().getMetadataMap())
                        .setNumber(reply0.getDriver().getPassport().getNumber())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Lists supplier contracts a page at a time.
     */
    public ListPendingSupplierContractsResponse listPendingSupplierContracts(ListPendingSupplierContractsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingSupplierContracts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierContract", request.getContext().getTraceId(), "ListPendingSupplierContracts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingSupplierContracts", request.getPage().getPageToken());
        return ListPendingSupplierContractsResponse.getDefaultInstance();
    }

    /**
     * Delegates a supplier contract.
     */
    public DelegateSupplierContractApprovalResponse delegateSupplierContractApproval(DelegateSupplierContractApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateSupplierContractApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var outbound0 = DeleteSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setEtag(request.getSupplierContract().getAudit().getEtag())
                .setReason(request.getReason())
                .setSupplierId(request.getSupplierContract().getCounterparty().getSupplierId())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).deleteSupplier(outbound0), rpcExecutor);

        var outbound1 = PauseRenewalWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(request.getSupplierContract().getBillingAddress().getCountryCode())
                                .setPostalCode(request.getSupplierContract().getBillingAddress().getPostalCode())
                                .setRecipient(request.getSupplierContract().getBillingAddress().getRecipient())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(request.getSupplierContract().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> contractsRenewalWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseRenewalWorkflow(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateSupplierContractApproval", request.getSupplierContract().getSupplierContractId());
        return DelegateSupplierContractApprovalResponse.newBuilder()
                .setOutcome(DelegateSupplierContractApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply1.getRenewal().getOrganizer().getEmail())
                                .build())
                        .putAllMetadata(request.getSupplierContract().getMetadataMap())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Recalls a supplier contract.
     */
    public RecallSupplierContractResponse recallSupplierContract(RecallSupplierContractRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallSupplierContract")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var outbound0 = CompareAuditTrailVersionsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getSupplierContract().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getSupplierContract().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getSupplierContract().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareAuditTrailVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply0 = auditAuditTrailHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareAuditTrailVersions(outbound0);

        List<ApproveMandateResponse> results1 = request.getSupplierContract().getLinesList().stream()
                .map(element -> ApproveMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .addLines(MandateLine.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .setTax(MandateLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(bankingMandateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveMandate(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? ApproveMandateResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = QuotePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePreferenceRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply1.getMandate().getBillingAddress().getLocality())
                                .setPostalCode(reply1.getMandate().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePreference(value));
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getSupplierContractId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallSupplierContract", request.getSupplierContract().getSupplierContractId());
        return RecallSupplierContractResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getPreference().getAudit().getUpdatedAt())
                                .build())
                        .setNumber(reply0.getAuditTrail().getNumber())
                        .setStatus(request.getSupplierContract().getStatus())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Returns a supplier contract.
     */
    public GetSupplierContractApprovalStatusResponse getSupplierContractApprovalStatus(GetSupplierContractApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetSupplierContractApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierContract", request.getSupplierContractId(), "GetSupplierContractApprovalStatus");

        var outbound0 = ListMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .setOwnerId(request.getContext().getTraceId())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ListMetricSeriesesResponse>();
        events.publishEvent(new GetSupplierContractApprovalStatusStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return GetSupplierContractApprovalStatusResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }

    /**
     * Comments a supplier contract.
     */
    public CommentSupplierContractApprovalResponse commentSupplierContractApproval(CommentSupplierContractApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentSupplierContractApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierContract(request.getContext().getTenantId(), request.getSupplierContract());

        var outbound0 = ApproveFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getSupplierContract().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsFixedAssetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveFixedAsset(outbound0), rpcExecutor);

        var outbound1 = PeekSupplierContractRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierContractId(request.getSupplierContract().getSupplierContractId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).peekSupplierContract(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = RpcSupport.unpack(Any.pack(CheckSupplierRatingEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setBank(SupplierRatingBankSettlement.newBuilder()
                                .setIban(request.getSupplierContract().getBank().getIban())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getSupplierContract().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build()), CheckSupplierRatingEligibilityRequest.class);
        var reply2 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkSupplierRatingEligibility(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSupplierContract().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentSupplierContractApproval", request.getSupplierContract().getNumber());
        return CommentSupplierContractApprovalResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getSupplierContract().getBillingAddress().getLocality())
                                .setPostalCode(reply2.getSupplierRating().getBillingAddress().getPostalCode())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getFixedAsset().getOwner().getDisplayName())
                                .build())
                        .putAllMetadata(request.getSupplierContract().getMetadataMap())
                        .setSupplierContractId(request.getSupplierContractId())
                        .build())
                .build();
    }
}
