package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersRequest;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.certificates.v1.Certificate;
import com.acme.certificates.v1.CertificateCalculationServiceGrpc;
import com.acme.certificates.v1.CsrRef;
import com.acme.certificates.v1.SimulateCertificateRequest;
import com.acme.certificates.v1.SimulateCertificateResponse;
import com.acme.collections.v1.PreviewWriteOffRequest;
import com.acme.collections.v1.PreviewWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffCardSettlement;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationRef;
import com.acme.compliance.v1.AttestationValidationServiceGrpc;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.BatchGetComplianceCasesRequest;
import com.acme.compliance.v1.BatchGetComplianceCasesResponse;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityRequest;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityResponse;
import com.acme.compliance.v1.CloneComplianceCaseRequest;
import com.acme.compliance.v1.CloneComplianceCaseResponse;
import com.acme.compliance.v1.CompareComplianceCaseOptionsRequest;
import com.acme.compliance.v1.CompareComplianceCaseOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseChannelOrigin;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.ComplianceCaseSla;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.CreateComplianceCaseRequest;
import com.acme.compliance.v1.CreateComplianceCaseResponse;
import com.acme.compliance.v1.DeleteComplianceCaseRequest;
import com.acme.compliance.v1.DeleteComplianceCaseResponse;
import com.acme.compliance.v1.ExplainAttestationDecisionRequest;
import com.acme.compliance.v1.ExplainAttestationDecisionResponse;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.GetComplianceCaseRequest;
import com.acme.compliance.v1.GetComplianceCaseResponse;
import com.acme.compliance.v1.LintComplianceCasesRequest;
import com.acme.compliance.v1.LintComplianceCasesResponse;
import com.acme.compliance.v1.ListComplianceCasesRequest;
import com.acme.compliance.v1.ListComplianceCasesResponse;
import com.acme.compliance.v1.PreviewAttestationRequest;
import com.acme.compliance.v1.PreviewAttestationResponse;
import com.acme.compliance.v1.QuoteControlRequest;
import com.acme.compliance.v1.QuoteControlResponse;
import com.acme.compliance.v1.SearchComplianceCasesRequest;
import com.acme.compliance.v1.SearchComplianceCasesResponse;
import com.acme.compliance.v1.SummarizeComplianceCasesRequest;
import com.acme.compliance.v1.SummarizeComplianceCasesResponse;
import com.acme.compliance.v1.UpdateComplianceCaseRequest;
import com.acme.compliance.v1.UpdateComplianceCaseResponse;
import com.acme.compliance.v1.WatchComplianceCasesRequest;
import com.acme.compliance.v1.WatchComplianceCasesResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.forecasting.v1.DemandForecastQueryServiceGrpc;
import com.acme.forecasting.v1.ResolveDemandForecastsRequest;
import com.acme.forecasting.v1.ResolveDemandForecastsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageBlockAction;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.EstimateCoverageRequest;
import com.acme.insurance.v1.EstimateCoverageResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.RecalculatePoliciesRequest;
import com.acme.insurance.v1.RecalculatePoliciesResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SendPostingBatchTestNoticeRequest;
import com.acme.ledger.v1.SendPostingBatchTestNoticeResponse;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.scheduling.v1.BrowseShiftsRequest;
import com.acme.scheduling.v1.BrowseShiftsResponse;
import com.acme.scheduling.v1.ShiftQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Manages compliance cases: creates, reads, changes, and lists them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ComplianceCaseApplication {
    private static final Logger log = LoggerFactory.getLogger(ComplianceCaseApplication.class);

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync;

    private final ChannelRegistry channels;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final AttestationValidationServiceGrpc.AttestationValidationServiceFutureStub complianceAttestationValidationServiceFuture;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceBlockingStub complianceComplianceCaseQueryServiceBlocking;

    private final ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final DemandForecastQueryServiceGrpc.DemandForecastQueryServiceFutureStub forecastingDemandForecastQueryServiceFuture;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final ComplianceRepository repository;

    private final Executor rpcExecutor;

    private final ShiftQueryServiceGrpc.ShiftQueryServiceBlockingStub schedulingShiftQueryServiceBlocking;

    public ComplianceCaseApplication(
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync,
            ChannelRegistry channels,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            AttestationValidationServiceGrpc.AttestationValidationServiceFutureStub complianceAttestationValidationServiceFuture,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceBlockingStub complianceComplianceCaseQueryServiceBlocking,
            ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            DemandForecastQueryServiceGrpc.DemandForecastQueryServiceFutureStub forecastingDemandForecastQueryServiceFuture,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            ComplianceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            ShiftQueryServiceGrpc.ShiftQueryServiceBlockingStub schedulingShiftQueryServiceBlocking) {
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingBankAccountServiceAsync = bankingBankAccountServiceAsync;
        this.channels = channels;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.complianceAttestationValidationServiceFuture = complianceAttestationValidationServiceFuture;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.complianceComplianceCaseQueryServiceBlocking = complianceComplianceCaseQueryServiceBlocking;
        this.complianceComplianceCaseValidationServiceBlocking = complianceComplianceCaseValidationServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.forecastingDemandForecastQueryServiceFuture = forecastingDemandForecastQueryServiceFuture;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.schedulingShiftQueryServiceBlocking = schedulingShiftQueryServiceBlocking;
    }

    /**
     * Creates a compliance case.
     */
    public CreateComplianceCaseResponse createComplianceCase(CreateComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = CompareComplianceCaseOptionsRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getComplianceCase().getRequester().getEmail())
                                .build())
                        .setCustomer(AttestationRef.newBuilder()
                                .setAttestationId(switch (request.getComplianceCase().getOriginCase()) {
                                    case CUSTOMER -> request.getComplianceCase().getCustomer().getAttestationId();
                                    default -> "";
                                })
                                .setDisplayName(switch (request.getComplianceCase().getOriginCase()) {
                                    case CUSTOMER -> request.getComplianceCase().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getComplianceCase().getRequester().getPhone())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareComplianceCaseOptions(outbound0);

        var outbound1 = CheckComplianceCaseEligibilityRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setChannel(ComplianceCaseChannelOrigin.newBuilder()
                                .setAddress(switch (request.getComplianceCase().getOriginCase()) {
                                    case CHANNEL -> request.getComplianceCase().getChannel().getAddress();
                                    default -> "";
                                })
                                .build())
                        .addComments(ComplianceCaseComment.newBuilder()
                                .setCreatedAt(reply0.getComplianceCase().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = complianceComplianceCaseValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkComplianceCaseEligibility(outbound1);

        var outbound2 = SimulateCertificateRequest.newBuilder()
                .setCertificate(Certificate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getComplianceCase().getAudit().getEtag())
                                .build())
                        .setCounterparty(CsrRef.newBuilder()
                                .setDisplayName(reply0.getComplianceCase().getCustomer().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.certificates.v1.CertificateCalculationService");
        var selected2 = CertificateCalculationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.simulateCertificate(outbound2);

        var outbound3 = SubscribePostingBatchUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribePostingBatchUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply1.getComplianceCase().getRequester().getEmail())
                                .setName(request.getComplianceCase().getRequester().getName())
                                .setPhone(reply1.getComplianceCase().getRequester().getPhone())
                                .setPreferredChannel(request.getComplianceCase().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply3 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribePostingBatchUpdates(outbound3);

        var outbound4 = BatchCreateBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBankAccountsRequest.StreamHeader.newBuilder()
                        .setSource(request.getComplianceCase().getAssignee().getUserId())
                        .setSubmittedBy(request.getComplianceCase().getRequester())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateBankAccountsResponse.getDefaultInstance());
        var input4 = bankingBankAccountServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBankAccounts(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getComplianceCase().getAudit().getCreatedAt())
                                .setCreatedBy(request.getComplianceCase().getAudit().getCreatedBy())
                                .setEtag(request.getComplianceCase().getAudit().getEtag())
                                .setUpdatedAt(request.getComplianceCase().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input5 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateComplianceCase", request.getComplianceCase().getSubject());
        return CreateComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getComplianceCase().getAssignee().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply3.getPostingBatch().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getComplianceCase().getAudit().getCreatedBy())
                                .build())
                        .addComments(ComplianceCaseComment.newBuilder()
                                .setCreatedAt(reply2.getCertificate().getAudit().getCreatedAt())
                                .build())
                        .setComplianceCaseId(request.getComplianceCase().getComplianceCaseId())
                        .setPriority(request.getComplianceCase().getPriority())
                        .setSla(ComplianceCaseSla.newBuilder()
                                .setBreached(reply1.getComplianceCase().getSla().getBreached())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns a compliance case.
     */
    public GetComplianceCaseResponse getComplianceCase(GetComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getComplianceCaseId(), "GetComplianceCase");

        var outbound0 = ExplainAttestationDecisionRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setCounterparty(ComplianceCaseRef.newBuilder()
                                .setComplianceCaseId(request.getComplianceCaseId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainAttestationDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var pending0 = complianceAttestationValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).explainAttestationDecision(outbound0);
        var reply0 = RpcSupport.await(pending0);
        return GetComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Changes a compliance case.
     */
    public UpdateComplianceCaseResponse updateComplianceCase(UpdateComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = QuoteControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getComplianceCase().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getComplianceCase().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getComplianceCase().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteControl(outbound0), rpcExecutor);

        var outbound1 = LintComplianceCasesRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getComplianceCase().getRequester().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> complianceComplianceCaseValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintComplianceCases(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return UpdateComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getControl().getAudit().getUpdatedBy())
                                .build())
                        .addComments(ComplianceCaseComment.newBuilder()
                                .setCreatedAt(reply1.getComplianceCase().getAudit().getCreatedAt())
                                .build())
                        .putAllCustomFields(request.getComplianceCase().getCustomFieldsMap())
                        .setPriority(reply1.getComplianceCase().getPriority())
                        .setRequester(request.getComplianceCase().getRequester())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Deletes a compliance case.
     */
    public DeleteComplianceCaseResponse deleteComplianceCase(DeleteComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getComplianceCaseId(), "DeleteComplianceCase");

        var outbound0 = JsonCodec.roundTrip(NotifyRetentionPolicyWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyRetentionPolicyWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build(), NotifyRetentionPolicyWatchersRequest.class);
        var reply0 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyRetentionPolicyWatchers(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteComplianceCase", request.getComplianceCaseId());
        return DeleteComplianceCaseResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists compliance cases a page at a time.
     */
    public ListComplianceCasesResponse listComplianceCases(ListComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getContext().getTraceId(), "ListComplianceCases");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListComplianceCases", request.getPage().getPageToken());
        return ListComplianceCasesResponse.getDefaultInstance();
    }

    /**
     * Returns several compliance cases at once.
     */
    public BatchGetComplianceCasesResponse batchGetComplianceCases(BatchGetComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getContext().getTraceId(), "BatchGetComplianceCases");
        return BatchGetComplianceCasesResponse.getDefaultInstance();
    }

    /**
     * Lists compliance cases a page at a time.
     */
    public SearchComplianceCasesResponse searchComplianceCases(SearchComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getContext().getTraceId(), "SearchComplianceCases");

        var outbound0 = BrowseShiftsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseShiftsRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> schedulingShiftQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseShifts(outbound0));

        var outbound1 = SummarizeComplianceCasesRequest.newBuilder()
                .putAllAnnotations(request.getFilter().getLabelsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeComplianceCasesRequest.Details.newBuilder()
                        .putAllFilters(reply0.getResult().getDetailsMap())
                        .build())
                .build();
        RpcPort<SummarizeComplianceCasesRequest, SummarizeComplianceCasesResponse> port1 = RpcPort.adapt(complianceComplianceCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::summarizeComplianceCases);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchComplianceCases", request.getPage().getPageToken());
        return SearchComplianceCasesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Streams the changes of compliance cases as they happen.
     */
    public WatchComplianceCasesResponse watchComplianceCases(WatchComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getContext().getTraceId(), "WatchComplianceCases");

        var builder0 = RecalculatePoliciesRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setPolicy(Policy.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setPhone(request.getSubscriber().getPhone())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePolicies(outbound0);

        var outbound1 = PreviewWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setCard(WriteOffCardSettlement.newBuilder()
                                .setNetwork(switch (reply0.getPolicy().getFundingCase()) {
                                    case CARD -> reply0.getPolicy().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewWriteOff(outbound1);

        var outbound2 = SendPostingBatchTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendPostingBatchTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setName(request.getSubscriber().getName())
                                .setPhone(request.getSubscriber().getPhone())
                                .build())
                        .build())
                .build();
        var command2 = new DispatchCommand<>("acme.ledger.v1.PostingBatchNotificationService/SendPostingBatchTestNotice", outbound2);
        var reply2 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendPostingBatchTestNotice(command2.payload());

        var outbound3 = BeanCopies.copy(AnnotateRetentionPolicyVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateRetentionPolicyVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getWriteOff().getAudit().getCreatedAt())
                                .setCreatedBy(reply2.getPostingBatch().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build(), AnnotateRetentionPolicyVersionRequest.class);
        var reply3 = auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateRetentionPolicyVersion(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchComplianceCases", request.getFilter().getQuery());
        return WatchComplianceCasesResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getPolicy().getHolder().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply3.getRetentionPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getSubscriber().getName())
                                .setPhone(request.getSubscriber().getPhone())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Takes a stream of compliance cases and answers once at the end.
     */
    public BatchCreateComplianceCasesResponse batchCreateComplianceCases(BatchCreateComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveComplianceCase(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = ResolveDemandForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = forecastingDemandForecastQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).resolveDemandForecasts(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateComplianceCases", request.getHeader().getSource());
        return BatchCreateComplianceCasesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Creates a compliance case.
     */
    public CloneComplianceCaseResponse cloneComplianceCase(CloneComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = ExplainComplianceCaseCalculationRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getComplianceCase().getCustomer().getDisplayName())
                                .build())
                        .setChannel(ComplianceCaseChannelOrigin.newBuilder()
                                .setChannel(request.getComplianceCase().getChannel().getChannel())
                                .build())
                        .setSla(ComplianceCaseSla.newBuilder()
                                .setBreached(request.getComplianceCase().getSla().getBreached())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            ExplainComplianceCaseCalculationResponse fetch(ExplainComplianceCaseCalculationRequest input) {
                return complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainComplianceCaseCalculation(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        List<EstimateCoverageResponse> results1 = request.getComplianceCase().getCommentsList().stream()
                .map(element -> EstimateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .setCreatedBy(request.getComplianceCase().getAudit().getCreatedBy())
                                .build())
                        .setBlock(CoverageBlockAction.newBuilder()
                                .setNotifyEmail(element.getAuthorEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getComplianceCase().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCoverage(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? EstimateCoverageResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = PreviewAttestationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewAttestationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getComplianceCase().getRequester().getEmail())
                                .setPhone(reply0.getComplianceCase().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.compliance.v1.AttestationValidationService");
        var serviceName2 = "acme.compliance.v1" + "." + "AttestationValidationService";
        var methodName2 = "Pre" + "viewAttestation";
        var descriptor2 = MethodDescriptor.<PreviewAttestationRequest, PreviewAttestationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(PreviewAttestationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(PreviewAttestationResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = (UnsubscribeRetentionPolicyUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeRetentionPolicyUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setName(request.getComplianceCase().getRequester().getName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeRetentionPolicyUpdates(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return CloneComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(request.getComplianceCase().getAssignee())
                        .setPriority(reply2.getAttestation().getCounterparty().getPriority())
                        .setRequester(request.getComplianceCase().getRequester())
                        .setSubject(reply0.getComplianceCase().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .build();
    }
}
