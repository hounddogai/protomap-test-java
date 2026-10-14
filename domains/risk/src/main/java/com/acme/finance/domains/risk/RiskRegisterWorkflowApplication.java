package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditTrailEligibilityRequest;
import com.acme.audit.v1.CheckAuditTrailEligibilityResponse;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityRequest;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityResponse;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.ComparePreferenceOptionsRequest;
import com.acme.customers.v1.ComparePreferenceOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CreateCustomerRequest;
import com.acme.customers.v1.CreateCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.ExplainConsentRecordDecisionRequest;
import com.acme.customers.v1.ExplainConsentRecordDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.risk.v1.AdvanceRiskRegisterWorkflowRequest;
import com.acme.risk.v1.AdvanceRiskRegisterWorkflowResponse;
import com.acme.risk.v1.BatchGetRiskRegistersRequest;
import com.acme.risk.v1.BatchGetRiskRegistersResponse;
import com.acme.risk.v1.CancelRiskRegisterWorkflowRequest;
import com.acme.risk.v1.CancelRiskRegisterWorkflowResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.GetRiskRegisterRulesRequest;
import com.acme.risk.v1.GetRiskRegisterRulesResponse;
import com.acme.risk.v1.InspectRiskRegisterWorkflowRequest;
import com.acme.risk.v1.InspectRiskRegisterWorkflowResponse;
import com.acme.risk.v1.PauseRiskRegisterWorkflowRequest;
import com.acme.risk.v1.PauseRiskRegisterWorkflowResponse;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowRequest;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RetryRiskRegisterWorkflowRequest;
import com.acme.risk.v1.RetryRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.ScoreExposureRequest;
import com.acme.risk.v1.ScoreExposureResponse;
import com.acme.risk.v1.ScoreRiskRegisterRequest;
import com.acme.risk.v1.ScoreRiskRegisterResponse;
import com.acme.risk.v1.SignalRiskRegisterWorkflowRequest;
import com.acme.risk.v1.SignalRiskRegisterWorkflowResponse;
import com.acme.risk.v1.StartRiskRegisterWorkflowRequest;
import com.acme.risk.v1.StartRiskRegisterWorkflowResponse;
import com.acme.risk.v1.UpdateRiskRegisterRequest;
import com.acme.risk.v1.UpdateRiskRegisterResponse;
import com.acme.search.v1.CountSearchIndexesRequest;
import com.acme.search.v1.CountSearchIndexesResponse;
import com.acme.search.v1.ExportSearchIndexesRequest;
import com.acme.search.v1.ExportSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a risk register from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class RiskRegisterWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(RiskRegisterWorkflowApplication.class);

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final ChannelRegistry channels;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking;

    private final RiskRegisterWorkflowRequestMapper mapper;

    private final RiskRepository repository;

    private final ExposureValidationServiceGrpc.ExposureValidationServiceBlockingStub riskExposureValidationServiceBlocking;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceBlockingStub riskRiskRegisterServiceBlocking;

    private final RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    public RiskRegisterWorkflowApplication(
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            ChannelRegistry channels,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            ApplicationEventPublisher events,
            LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking,
            RiskRegisterWorkflowRequestMapper mapper,
            RiskRepository repository,
            ExposureValidationServiceGrpc.ExposureValidationServiceBlockingStub riskExposureValidationServiceBlocking,
            RiskRegisterServiceGrpc.RiskRegisterServiceBlockingStub riskRiskRegisterServiceBlocking,
            RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking) {
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.channels = channels;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.customersCustomerServiceBlocking = customersCustomerServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.events = events;
        this.lendingLoanApprovalServiceBlocking = lendingLoanApprovalServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskExposureValidationServiceBlocking = riskExposureValidationServiceBlocking;
        this.riskRiskRegisterServiceBlocking = riskRiskRegisterServiceBlocking;
        this.riskRiskRegisterValidationServiceBlocking = riskRiskRegisterValidationServiceBlocking;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
    }

    public record CancelRiskRegisterWorkflowStage0Requested(RejectCollectionCaseRequest request, CompletableFuture<RejectCollectionCaseResponse> completion, io.grpc.Context context) {}

    public record PauseRiskRegisterWorkflowStage2Requested(ComparePreferenceOptionsRequest request, CompletableFuture<ComparePreferenceOptionsResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a risk register.
     */
    public StartRiskRegisterWorkflowResponse startRiskRegisterWorkflow(StartRiskRegisterWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartRiskRegisterWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var builder0 = ScoreExposureRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setExposure(Exposure.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedBy(request.getRiskRegister().getAudit().getUpdatedBy())
                        .build())
                .setContact(ContactInfo.newBuilder()
                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = riskExposureValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreExposure(outbound0);

        var outbound1 = StreamRetentionPolicyNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getRiskRegister().getManager().getEmail())
                        .setPhone(reply0.getExposure().getContact().getPhone())
                        .build())
                .build();
        var stream1 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyNotices(outbound1);
        var reply1 = StreamRetentionPolicyNoticesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = mapper.startRiskRegisterWorkflowStage2(request, reply0);
        var reply2 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRiskRegister(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartRiskRegisterWorkflow", request.getRiskRegister().getRiskRegisterId());
        return StartRiskRegisterWorkflowResponse.newBuilder()
                .setOutcome(StartRiskRegisterWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .setDepth(reply2.getRiskRegister().getDepth())
                        .setLocation(request.getRiskRegister().getLocation())
                        .setManager(UserRef.newBuilder()
                                .setEmail(reply0.getExposure().getContact().getEmail())
                                .build())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Advances a risk register.
     */
    public AdvanceRiskRegisterWorkflowResponse advanceRiskRegisterWorkflow(AdvanceRiskRegisterWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceRiskRegisterWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var builder0 = CheckHouseholdEligibilityRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setHousehold(Household.newBuilder()
                .addChildren(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .setLocation(PostalAddress.newBuilder()
                                        .setPostalCode(request.getRiskRegister().getLocation().getPostalCode())
                                        .build())
                                .build())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkHouseholdEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceRiskRegisterWorkflow", request.getRiskRegister().getRiskRegisterId());
        return AdvanceRiskRegisterWorkflowResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .setName(reply0.getHousehold().getName())
                                .build())
                        .setParentId(request.getRiskRegister().getParentId())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Cancels a risk register.
     */
    public CancelRiskRegisterWorkflowResponse cancelRiskRegisterWorkflow(CancelRiskRegisterWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelRiskRegisterWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = RejectCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getRiskRegister().getManager().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RejectCollectionCaseRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<RejectCollectionCaseResponse>();
        events.publishEvent(new CancelRiskRegisterWorkflowStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = CheckAuditTrailEligibilityRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getRiskRegister().getLocation().getRecipient())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.audit.v1.AuditTrailValidationService");
        var selected1 = AuditTrailValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.checkAuditTrailEligibility(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelRiskRegisterWorkflow", request.getRiskRegister().getRiskRegisterId());
        return CancelRiskRegisterWorkflowResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply1.getAuditTrail().getAudit().getCreatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply0.getCollectionCase().getCustomer().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRecipient(reply1.getAuditTrail().getBillingAddress().getRecipient())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setEmail(reply0.getCollectionCase().getRequester().getEmail())
                                .build())
                        .setName(request.getRiskRegister().getName())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Retrys a risk register.
     */
    public RetryRiskRegisterWorkflowResponse retryRiskRegisterWorkflow(RetryRiskRegisterWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryRiskRegisterWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = CheckCustomerSegmentEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getRiskRegister().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getRiskRegister().getManager().getEmail())
                                .build())
                        .build())
                .build();
        Function<CheckCustomerSegmentEligibilityRequest, CheckCustomerSegmentEligibilityResponse> operation0 = customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::checkCustomerSegmentEligibility;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = UpdateRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(request.getRiskRegister().getAudit().getCreatedBy())
                                                .setUpdatedAt(reply0.getCustomerSegment().getAudit().getUpdatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply0.getCustomerSegment().getOwner().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(riskRiskRegisterServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateRiskRegister(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryRiskRegisterWorkflow", request.getRiskRegister().getRiskRegisterId());
        return RetryRiskRegisterWorkflowResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(reply0.getCustomerSegment().getAudit().getCreatedBy())
                                                .setUpdatedBy(reply0.getCustomerSegment().getAudit().getUpdatedBy())
                                                .build())
                                        .addChildren(RiskRegister.newBuilder()
                                                .setRiskRegisterId(reply1.getRiskRegister().getRiskRegisterId())
                                                .build())
                                        .build())
                                .build())
                        .setDepth(request.getRiskRegister().getDepth())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Pauses a risk register.
     */
    public PauseRiskRegisterWorkflowResponse pauseRiskRegisterWorkflow(PauseRiskRegisterWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseRiskRegisterWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = (ScoreRiskRegisterRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setPostalCode(request.getRiskRegister().getLocation().getPostalCode())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getNotify().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = riskRiskRegisterValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRiskRegister(outbound0);

        var outbound1 = CreateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getRiskRegister().getLocation().getLinesList())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(reply0.getRiskRegister().getManager().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> customersCustomerServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createCustomer(value)).orElseThrow();

        var outbound2 = ComparePreferenceOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getRiskRegister().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getRiskRegister().getManager().getDisplayName())
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<ComparePreferenceOptionsResponse>();
        events.publishEvent(new PauseRiskRegisterWorkflowStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseRiskRegisterWorkflow", request.getRiskRegister().getRiskRegisterId());
        return PauseRiskRegisterWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(reply0.getRiskRegister().getAudit().getCreatedBy())
                                                .build())
                                        .build())
                                .setName(reply2.getPreference().getName())
                                .build())
                        .setLocation(request.getRiskRegister().getLocation())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Resumes a risk register.
     */
    public ResumeRiskRegisterWorkflowResponse resumeRiskRegisterWorkflow(ResumeRiskRegisterWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeRiskRegisterWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = GetRiskRegisterRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegisterId(request.getRiskRegisterId())
                .build();
        var channel0 = channels.lookup("acme.risk.v1.RiskRegisterValidationService");
        var selected0 = RiskRegisterValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.getRiskRegisterRules(outbound0);

        var outbound1 = mapper.resumeRiskRegisterWorkflowStage1(request, reply0);
        var reply1 = lendingLoanApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentLoanApproval(outbound1);

        var outbound2 = ExportSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSearchIndexesRequest.Filter.newBuilder()
                        .setRegion(reply0.getRiskRegister().getLocation().getRegion())
                        .build())
                .build();
        var stream2 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSearchIndexes(outbound2);
        var reply2 = ExportSearchIndexesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeRiskRegisterWorkflow", request.getRiskRegister().getRiskRegisterId());
        return ResumeRiskRegisterWorkflowResponse.newBuilder()
                .setRiskRegister(RiskRegister.newBuilder()
                        .putAllAttributes(request.getRiskRegister().getAttributesMap())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getLoan().getAudit().getUpdatedAt())
                                .build())
                        .addChildren(RiskRegister.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setCreatedAt(reply1.getLoan().getAudit().getCreatedAt())
                                        .build())
                                .addChildren(RiskRegister.newBuilder()
                                        .setRiskRegisterId(reply0.getRiskRegister().getRiskRegisterId())
                                        .build())
                                .build())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Returns a risk register.
     */
    public InspectRiskRegisterWorkflowResponse inspectRiskRegisterWorkflow(InspectRiskRegisterWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectRiskRegisterWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RiskRegister", request.getRiskRegisterId(), "InspectRiskRegisterWorkflow");

        var outbound0 = CommentLoanApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentLoanApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.lending.v1.LoanApprovalService");
        var selected0 = LoanApprovalServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.commentLoanApproval(outbound0);
        return InspectRiskRegisterWorkflowResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getLoan().getAudit().getUpdatedBy())
                                .build())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .build())
                .build();
    }

    /**
     * Signals a risk register.
     */
    public SignalRiskRegisterWorkflowResponse signalRiskRegisterWorkflow(SignalRiskRegisterWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalRiskRegisterWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRiskRegister(request.getContext().getTenantId(), request.getRiskRegister());

        var outbound0 = JsonCodec.roundTrip(CountSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRiskRegister().getAudit().getCreatedAt())
                        .setRegion(request.getRiskRegister().getLocation().getRegion())
                        .build())
                .build(), CountSearchIndexesRequest.class);
        var reply0 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSearchIndexes(outbound0);

        var outbound1 = ExplainConsentRecordDecisionRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainConsentRecordDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainConsentRecordDecision(value));

        var outbound2 = BatchGetRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.risk.v1.RiskRegisterService");
        var selected2 = RiskRegisterServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.batchGetRiskRegisters(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRiskRegister().getRiskRegisterId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalRiskRegisterWorkflow", request.getRiskRegister().getRiskRegisterId());
        return SignalRiskRegisterWorkflowResponse.newBuilder()
                .setOutcome(SignalRiskRegisterWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .addChildren(RiskRegister.newBuilder()
                                                .putAllAttributes(reply1.getConsentRecord().getAttributesMap())
                                                .build())
                                        .build())
                                .build())
                        .setRiskRegisterId(request.getRiskRegisterId())
                        .setStatus(request.getRiskRegister().getStatus())
                        .build())
                .build();
    }
}
