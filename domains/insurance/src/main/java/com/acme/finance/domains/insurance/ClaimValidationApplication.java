package com.acme.finance.domains.insurance;

import com.acme.appointments.v1.AppointmentQueryServiceGrpc;
import com.acme.appointments.v1.ResolveAppointmentsRequest;
import com.acme.appointments.v1.ResolveAppointmentsResponse;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.ScheduleAuditTrailExportRequest;
import com.acme.audit.v1.ScheduleAuditTrailExportResponse;
import com.acme.chat.v1.ChatRoom;
import com.acme.chat.v1.ChatRoomServiceGrpc;
import com.acme.chat.v1.CreateChatRoomRequest;
import com.acme.chat.v1.CreateChatRoomResponse;
import com.acme.chat.v1.SearchChatRoomsRequest;
import com.acme.chat.v1.SearchChatRoomsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateClaimRequest;
import com.acme.insurance.v1.CalculateClaimResponse;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.CheckClaimEligibilityRequest;
import com.acme.insurance.v1.CheckClaimEligibilityResponse;
import com.acme.insurance.v1.CheckPolicyEligibilityRequest;
import com.acme.insurance.v1.CheckPolicyEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimSla;
import com.acme.insurance.v1.CompareClaimOptionsRequest;
import com.acme.insurance.v1.CompareClaimOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.EstimateCoverageRequest;
import com.acme.insurance.v1.EstimateCoverageResponse;
import com.acme.insurance.v1.ExplainClaimDecisionRequest;
import com.acme.insurance.v1.ExplainClaimDecisionResponse;
import com.acme.insurance.v1.GetClaimRulesRequest;
import com.acme.insurance.v1.GetClaimRulesResponse;
import com.acme.insurance.v1.GetPolicyRulesRequest;
import com.acme.insurance.v1.GetPolicyRulesResponse;
import com.acme.insurance.v1.LintClaimsRequest;
import com.acme.insurance.v1.LintClaimsResponse;
import com.acme.insurance.v1.NormalizeClaimRequest;
import com.acme.insurance.v1.NormalizeClaimResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.PreviewClaimRequest;
import com.acme.insurance.v1.PreviewClaimResponse;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.insurance.v1.RecalculatePoliciesRequest;
import com.acme.insurance.v1.RecalculatePoliciesResponse;
import com.acme.insurance.v1.ScoreClaimRequest;
import com.acme.insurance.v1.ScoreClaimResponse;
import com.acme.insurance.v1.SimulateCoverageRequest;
import com.acme.insurance.v1.SimulateCoverageResponse;
import com.acme.insurance.v1.SimulatePolicyRequest;
import com.acme.insurance.v1.SimulatePolicyResponse;
import com.acme.insurance.v1.ValidateClaimRequest;
import com.acme.insurance.v1.ValidateClaimResponse;
import com.acme.insurance.v1.ValidatePolicyRequest;
import com.acme.insurance.v1.ValidatePolicyResponse;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesResponse;
import com.acme.ticketing.v1.CloneTicketRequest;
import com.acme.ticketing.v1.CloneTicketResponse;
import com.acme.ticketing.v1.Ticket;
import com.acme.ticketing.v1.TicketBankSettlement;
import com.acme.ticketing.v1.TicketServiceGrpc;
import com.acme.venues.v1.VenueRef;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Checks claims against the rules before anything acts on them. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class ClaimValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(ClaimValidationApplication.class);

    private final AppointmentQueryServiceGrpc.AppointmentQueryServiceBlockingStub appointmentsAppointmentQueryServiceBlocking;

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final ChatRoomServiceGrpc.ChatRoomServiceBlockingStub chatChatRoomServiceBlocking;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceFutureStub insurancePolicyCalculationServiceFuture;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final ClaimValidationRequestMapper mapper;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final TicketServiceGrpc.TicketServiceBlockingStub ticketingTicketServiceBlocking;

    public ClaimValidationApplication(
            AppointmentQueryServiceGrpc.AppointmentQueryServiceBlockingStub appointmentsAppointmentQueryServiceBlocking,
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            ChatRoomServiceGrpc.ChatRoomServiceBlockingStub chatChatRoomServiceBlocking,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceFutureStub insurancePolicyCalculationServiceFuture,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            ClaimValidationRequestMapper mapper,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            TicketServiceGrpc.TicketServiceBlockingStub ticketingTicketServiceBlocking) {
        this.appointmentsAppointmentQueryServiceBlocking = appointmentsAppointmentQueryServiceBlocking;
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.chatChatRoomServiceBlocking = chatChatRoomServiceBlocking;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.insurancePolicyCalculationServiceFuture = insurancePolicyCalculationServiceFuture;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.ticketingTicketServiceBlocking = ticketingTicketServiceBlocking;
    }

    /**
     * Validates a claim.
     */
    public ValidateClaimResponse validateClaim(ValidateClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = CalculatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getClaim().getAudit().getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(switch (request.getClaim().getOriginCase()) {
                                    case CUSTOMER -> request.getClaim().getCustomer().getEmail();
                                    default -> "";
                                })
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(request.getClaim().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePolicy(outbound0);

        var outbound1 = QuoteCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getClaim().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<QuoteCoverageRequest, QuoteCoverageResponse>() {
            @Override
            public QuoteCoverageResponse apply(QuoteCoverageRequest input) {
                return insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCoverage(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = CheckPolicyEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckPolicyEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getPolicy().getContact().getName())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(switch (request.getClaim().getOriginCase()) {
                                    case CUSTOMER -> request.getClaim().getCustomer().getEmail();
                                    default -> "";
                                })
                                .build())
                        .addLimits(PolicyLimit.newBuilder()
                                .setAmount(reply1.getCoverage().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var reply2 = insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPolicyEligibility(outbound2);

        var outbound3 = RecalculatePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending3 = insurancePolicyCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePolicies(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateClaim", request.getClaim().getClaimId());
        return ValidateClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(request.getClaim().getAssignee())
                        .setClaimId(request.getClaimId())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                .build())
                        .setSla(ClaimSla.newBuilder()
                                .setPolicyId(reply3.getPolicy().getPolicyId())
                                .build())
                        .build())
                .setOutcome(ValidateClaimResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Previews a claim.
     */
    public PreviewClaimResponse previewClaim(PreviewClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = QuotePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getClaim().getAudit().getEtag())
                                .setUpdatedBy(request.getClaim().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePolicy(outbound0), rpcExecutor);

        var outbound1 = ScheduleAuditTrailExportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleAuditTrailExport(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = SimulatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                .setPreferredChannel(request.getClaim().getRequester().getPreferredChannel())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(switch (request.getClaim().getOriginCase()) {
                                    case CUSTOMER -> request.getClaim().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePolicy(outbound2);

        var outbound3 = GetPolicyRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicyId(request.getClaim().getSla().getPolicyId())
                .build();
        var command3 = new DispatchCommand<>("acme.insurance.v1.PolicyValidationService/GetPolicyRules", outbound3);
        var reply3 = insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getPolicyRules(command3.payload());
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewClaim", request.getClaim().getClaimId());
        return PreviewClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setClaimId(request.getClaimId())
                        .addComments(ClaimComment.newBuilder()
                                .setCreatedAt(reply3.getPolicy().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply2.getPolicy().getContact().getEmail())
                                .build())
                        .setSubject(request.getClaim().getSubject())
                        .build())
                .setOutcome(PreviewClaimResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Checks a claim.
     */
    public CheckClaimEligibilityResponse checkClaimEligibility(CheckClaimEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckClaimEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = CompareClaimOptionsRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getClaim().getAssignee().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareClaimOptions(outbound0);

        var outbound1 = QuotePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getClaim().getAudit().getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getClaim().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            QuotePolicyResponse fetch(QuotePolicyRequest input) {
                return insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePolicy(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = CalculateClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getClaim().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply1.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateClaim(value));

        var outbound3 = CreateChatRoomRequest.newBuilder()
                .setChatRoom(ChatRoom.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getClaim().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(switch (request.getClaim().getOriginCase()) {
                                    case CUSTOMER -> request.getClaim().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .setEmail(reply2.getClaim().getAssignee().getEmail())
                                .setUserId(request.getClaim().getAssignee().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply3 = chatChatRoomServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createChatRoom(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckClaimEligibility", request.getClaim().getClaimId());
        return CheckClaimEligibilityResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply2.getClaim().getCustomer().getEmail())
                                .setUserId(reply3.getChatRoom().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setClaimId(request.getClaimId())
                        .putAllCustomFields(request.getClaim().getCustomFieldsMap())
                        .setSla(ClaimSla.newBuilder()
                                .setPausedFor(reply0.getClaim().getSla().getPausedFor())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Normalizes a claim.
     */
    public NormalizeClaimResponse normalizeClaim(NormalizeClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var builder0 = ValidatePolicyRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setPolicy(Policy.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedAt(request.getClaim().getAudit().getUpdatedAt())
                        .build())
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getContact().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePolicy(outbound0);

        var outbound1 = QuotePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setCard(PolicyCardFunding.newBuilder()
                                .setHolderName(switch (reply0.getPolicy().getFundingCase()) {
                                    case CARD -> reply0.getPolicy().getCard().getHolderName();
                                    default -> "";
                                })
                                .setToken(switch (reply0.getPolicy().getFundingCase()) {
                                    case CARD -> reply0.getPolicy().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(reply0.getPolicy().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePolicy(outbound1);

        List<CloneTicketResponse> results2 = request.getClaim().getCommentsList().stream()
                .map(element -> CloneTicketRequest.newBuilder()
                .setContext(request.getContext())
                .setTicket(Ticket.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setBank(TicketBankSettlement.newBuilder()
                                .setAccountHolder(reply0.getPolicy().getBank().getAccountHolder())
                                .setBic(reply1.getPolicy().getBank().getBic())
                                .build())
                        .setCounterparty(VenueRef.newBuilder()
                                .setDisplayName(reply1.getPolicy().getHolder().getDisplayName())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(ticketingTicketServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneTicket(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? CloneTicketResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeClaim", request.getClaim().getClaimId());
        return NormalizeClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setRequester(request.getClaim().getRequester())
                        .build())
                .setOutcome(NormalizeClaimResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a claim.
     */
    public ExplainClaimDecisionResponse explainClaimDecision(ExplainClaimDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainClaimDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = RpcSupport.unpack(Any.pack(SubscribeJournalEntryUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeJournalEntryUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setName(request.getClaim().getRequester().getName())
                                .build())
                        .build())
                .build()), SubscribeJournalEntryUpdatesRequest.class);
        var reply0 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeJournalEntryUpdates(outbound0);

        var outbound1 = QuotePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setBank(PolicyBankFunding.newBuilder()
                                .setBic(switch (reply0.getJournalEntry().getSettlementCase()) {
                                    case BANK -> reply0.getJournalEntry().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePolicy(outbound1);

        var outbound2 = SimulateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setDetails(SimulateCoverageRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(reply0.getJournalEntry().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<SimulateCoverageRequest, SimulateCoverageResponse>() {
            @Override
            public SimulateCoverageResponse apply(SimulateCoverageRequest input) {
                return insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCoverage(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainClaimDecision", request.getClaim().getClaimId());
        return ExplainClaimDecisionResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getJournalEntry().getAudit().getEtag())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply1.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .setSubject(request.getClaim().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Scores a claim.
     */
    public ScoreClaimResponse scoreClaim(ScoreClaimRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreClaim")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = CalculatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculatePolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePolicy(outbound0), rpcExecutor);

        var outbound1 = QuotePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePolicy(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var builder2 = SearchChatRoomsRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setFilter(SearchChatRoomsRequest.Filter.newBuilder()
                .setCreatedAfter(reply0.getPolicy().getAudit().getUpdatedAt())
                .putAllLabels(reply1.getPolicy().getLabelsMap())
                .setOwnerId(request.getClaim().getClaimId())
                .setRegion(request.getDetails().getDestination().getRegion())
                .build());
        var outbound2 = builder2.build();
        var reply2 = chatChatRoomServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchChatRooms(outbound2);

        var outbound3 = EstimateCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getPolicy().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCoverage(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getClaim().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreClaim", request.getClaim().getSubject());
        return ScoreClaimResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply3.getCoverage().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setPriority(request.getClaim().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints a claim.
     */
    public LintClaimsResponse lintClaims(LintClaimsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintClaims")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveClaim(request.getContext().getTenantId(), request.getClaim());

        var outbound0 = RecalculatePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getClaim().getAudit().getCreatedBy())
                                .setEtag(request.getClaim().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getClaim().getAssignee().getEmail())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(request.getClaim().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<RecalculatePoliciesRequest, RecalculatePoliciesResponse> port0 = RpcPort.adapt(insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculatePolicies);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = mapper.lintClaimsStage1(request, reply0);
        var reply1 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCoverage(outbound1);

        var outbound2 = QuotePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            QuotePolicyResponse fetch(QuotePolicyRequest input) {
                return insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePolicy(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = ResolveAppointmentsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(appointmentsAppointmentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAppointments(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getClaim().getClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintClaims", request.getClaim().getClaimId());
        return LintClaimsResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCoverage().getAudit().getEtag())
                                .build())
                        .setClaimId(request.getClaimId())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(reply2.getPolicy().getContact().getName())
                                .build())
                        .setSubject(request.getClaim().getSubject())
                        .build())
                .setOutcome(LintClaimsResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a claim.
     */
    public GetClaimRulesResponse getClaimRules(GetClaimRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetClaimRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Claim", request.getClaimId(), "GetClaimRules");

        var outbound0 = QuoteCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            QuoteCoverageResponse fetch(QuoteCoverageRequest input) {
                return insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCoverage(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        return GetClaimRulesResponse.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCoverage().getAudit().getCreatedBy())
                                .build())
                        .setClaimId(request.getClaimId())
                        .build())
                .build();
    }
}
