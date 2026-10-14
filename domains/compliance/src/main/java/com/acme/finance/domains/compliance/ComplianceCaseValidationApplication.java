package com.acme.finance.domains.compliance;

import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BrowseBankAccountsRequest;
import com.acme.banking.v1.BrowseBankAccountsResponse;
import com.acme.banking.v1.FindBankAccountsRequest;
import com.acme.banking.v1.FindBankAccountsResponse;
import com.acme.chat.v1.ChatRoom;
import com.acme.chat.v1.ChatRoomCalculationServiceGrpc;
import com.acme.chat.v1.CompareChatRoomOptionsRequest;
import com.acme.chat.v1.CompareChatRoomOptionsResponse;
import com.acme.chat.v1.RecalculateChatRoomsRequest;
import com.acme.chat.v1.RecalculateChatRoomsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityRequest;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityResponse;
import com.acme.compliance.v1.CompareComplianceCaseOptionsRequest;
import com.acme.compliance.v1.CompareComplianceCaseOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseSla;
import com.acme.compliance.v1.ComplianceCaseStatusChange;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlBlockAction;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.ExplainComplianceCaseDecisionRequest;
import com.acme.compliance.v1.ExplainComplianceCaseDecisionResponse;
import com.acme.compliance.v1.GetComplianceCaseRulesRequest;
import com.acme.compliance.v1.GetComplianceCaseRulesResponse;
import com.acme.compliance.v1.LintComplianceCasesRequest;
import com.acme.compliance.v1.LintComplianceCasesResponse;
import com.acme.compliance.v1.NormalizeComplianceCaseRequest;
import com.acme.compliance.v1.NormalizeComplianceCaseResponse;
import com.acme.compliance.v1.PreviewComplianceCaseRequest;
import com.acme.compliance.v1.PreviewComplianceCaseResponse;
import com.acme.compliance.v1.QuoteControlRequest;
import com.acme.compliance.v1.QuoteControlResponse;
import com.acme.compliance.v1.RecalculateControlsRequest;
import com.acme.compliance.v1.RecalculateControlsResponse;
import com.acme.compliance.v1.SimulateComplianceCaseRequest;
import com.acme.compliance.v1.SimulateComplianceCaseResponse;
import com.acme.compliance.v1.ValidateComplianceCaseRequest;
import com.acme.compliance.v1.ValidateComplianceCaseResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.BrowsePoliciesRequest;
import com.acme.insurance.v1.BrowsePoliciesResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesResponse;
import com.acme.lending.v1.CalculateCollateralRequest;
import com.acme.lending.v1.CalculateCollateralResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralDimensions;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.acme.messaging.v1.ChannelRef;
import com.acme.messaging.v1.CompareConversationOptionsRequest;
import com.acme.messaging.v1.CompareConversationOptionsResponse;
import com.acme.messaging.v1.Conversation;
import com.acme.messaging.v1.ConversationCalculationServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
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
 * Checks compliance cases against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ComplianceCaseValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(ComplianceCaseValidationApplication.class);

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking;

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceFutureStub bankingBankAccountQueryServiceFuture;

    private final ChatRoomCalculationServiceGrpc.ChatRoomCalculationServiceBlockingStub chatChatRoomCalculationServiceBlocking;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking;

    private final PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking;

    private final ConversationCalculationServiceGrpc.ConversationCalculationServiceBlockingStub messagingConversationCalculationServiceBlocking;

    private final ComplianceRepository repository;

    private final Executor rpcExecutor;

    public ComplianceCaseValidationApplication(
            BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking,
            BankAccountQueryServiceGrpc.BankAccountQueryServiceFutureStub bankingBankAccountQueryServiceFuture,
            ChatRoomCalculationServiceGrpc.ChatRoomCalculationServiceBlockingStub chatChatRoomCalculationServiceBlocking,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking,
            PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking,
            ConversationCalculationServiceGrpc.ConversationCalculationServiceBlockingStub messagingConversationCalculationServiceBlocking,
            ComplianceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.bankingBankAccountQueryServiceBlocking = bankingBankAccountQueryServiceBlocking;
        this.bankingBankAccountQueryServiceFuture = bankingBankAccountQueryServiceFuture;
        this.chatChatRoomCalculationServiceBlocking = chatChatRoomCalculationServiceBlocking;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.complianceControlCalculationServiceBlocking = complianceControlCalculationServiceBlocking;
        this.insurancePolicyQueryServiceBlocking = insurancePolicyQueryServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.lendingCollateralCalculationServiceBlocking = lendingCollateralCalculationServiceBlocking;
        this.messagingConversationCalculationServiceBlocking = messagingConversationCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Validates a compliance case.
     */
    public ValidateComplianceCaseResponse validateComplianceCase(ValidateComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = BeanCopies.copy(RecalculateChatRoomsRequest.newBuilder()
                .setChatRoom(ChatRoom.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getComplianceCase().getAudit().getCreatedAt())
                                .setEtag(request.getComplianceCase().getAudit().getEtag())
                                .setUpdatedAt(request.getComplianceCase().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), RecalculateChatRoomsRequest.class);
        var reply0 = chatChatRoomCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateChatRooms(outbound0);

        var outbound1 = JsonCodec.roundTrip(CalculateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getChatRoom().getAddress().getCountryCode())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getComplianceCase().getAudit().getUpdatedBy())
                                .build())
                        .setDimensions(CollateralDimensions.newBuilder()
                                .setHeight(reply0.getChatRoom().getDimensions().getHeight())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateCollateralRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getChatRoom().getAddress().getLinesList())
                                .build())
                        .build())
                .build(), CalculateCollateralRequest.class);
        var reply1 = lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCollateral(outbound1);

        var outbound2 = CompareConversationOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setConversation(Conversation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getCollateral().getOwner().getDisplayName())
                                .build())
                        .setCustomer(ChannelRef.newBuilder()
                                .setDisplayName(switch (request.getComplianceCase().getOriginCase()) {
                                    case CUSTOMER -> request.getComplianceCase().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setDetails(CompareConversationOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(switch (reply1.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> reply1.getCollateral().getAddress().getLocality();
                                    default -> "";
                                })
                                .setRecipient(switch (reply1.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> reply1.getCollateral().getAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = messagingConversationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareConversationOptions(outbound2);

        var outbound3 = CompareComplianceCaseOptionsRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getCollateral().getOwner().getEmail())
                                .build())
                        .setSla(ComplianceCaseSla.newBuilder()
                                .setPausedFor(request.getComplianceCase().getSla().getPausedFor())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareComplianceCaseOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getChatRoom().getAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareComplianceCaseOptions(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return ValidateComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply2.getConversation().getAssignee().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getChatRoom().getAudit().getEtag())
                                .build())
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setPriority(request.getComplianceCase().getPriority())
                        .setSla(ComplianceCaseSla.newBuilder()
                                .setBreached(reply3.getComplianceCase().getSla().getBreached())
                                .build())
                        .build())
                .setOutcome(ValidateComplianceCaseResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Previews a compliance case.
     */
    public PreviewComplianceCaseResponse previewComplianceCase(PreviewComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = FindBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindBankAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getComplianceCase().getAudit().getCreatedAt())
                        .putAllLabels(request.getComplianceCase().getCustomFieldsMap())
                        .setOwnerId(request.getComplianceCase().getAssignee().getUserId())
                        .build())
                .build();
        var pending0 = bankingBankAccountQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).findBankAccounts(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var futures1 = request.getComplianceCase().getHistoryList().stream()
                .map(element -> SimulateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .addHistory(ComplianceCaseStatusChange.newBuilder()
                                .setToStatus(element.getToStatus())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateComplianceCase(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? SimulateComplianceCaseResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = QuoteControlRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getComplianceCase().getAssignee().getDisplayName())
                                .setEmail(reply1.getComplianceCase().getAssignee().getEmail())
                                .setUserId(request.getComplianceCase().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var reply2 = complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteControl(outbound2);

        var futures3 = request.getComplianceCase().getCommentsList().stream()
                .map(element -> RecalculateControlsRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setBlock(ControlBlockAction.newBuilder()
                                .setNotifyEmail(element.getAuthorEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getComplianceCase().getAssignee().getEmail())
                                .setUserId(reply1.getComplianceCase().getAssignee().getUserId())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> complianceControlCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateControls(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? RecalculateControlsResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return PreviewComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply2.getControl().getOwner().getDisplayName())
                                .build())
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setSubject(request.getComplianceCase().getSubject())
                        .build())
                .setOutcome(PreviewComplianceCaseResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Checks a compliance case.
     */
    public CheckComplianceCaseEligibilityResponse checkComplianceCaseEligibility(CheckComplianceCaseEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckComplianceCaseEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = SimulateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getComplianceCase().getAudit().getCreatedBy())
                                .setEtag(request.getComplianceCase().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getComplianceCase().getAssignee().getEmail())
                                .setUserId(request.getComplianceCase().getAssignee().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<SimulateCollateralRequest, SimulateCollateralResponse> operation0 = lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::simulateCollateral;
        var reply0 = operation0.apply(outbound0);

        var builder1 = SubscribeJournalEntryUpdatesRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setDetails(SubscribeJournalEntryUpdatesRequest.Details.newBuilder()
                .setPayload(Any.pack(request.getContext()))
                .setRecipient(ContactInfo.newBuilder()
                        .setEmail(request.getComplianceCase().getAssignee().getEmail())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeJournalEntryUpdates(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckComplianceCaseEligibility", request.getComplianceCase().getComplianceCaseId());
        return CheckComplianceCaseEligibilityResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getCollateral().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getCollateral().getAudit().getUpdatedBy())
                                .build())
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setSubject(request.getComplianceCase().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Normalizes a compliance case.
     */
    public NormalizeComplianceCaseResponse normalizeComplianceCase(NormalizeComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = ExplainComplianceCaseCalculationRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getComplianceCase().getRequester().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainComplianceCaseCalculation(value)))
                .collect(Collectors.toList()).getFirst();

        List<BrowsePoliciesResponse> results1 = request.getComplianceCase().getCommentsList().stream()
                .map(element -> BrowsePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowsePoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getCreatedAt())
                        .putAllLabels(reply0.getComplianceCase().getCustomFieldsMap())
                        .setOwnerId(element.getCommentId())
                        .build())
                .build())
                .flatMap(value -> Stream.of(insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browsePolicies(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? BrowsePoliciesResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return NormalizeComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setRequester(request.getComplianceCase().getRequester())
                        .build())
                .setOutcome(NormalizeComplianceCaseResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a compliance case.
     */
    public ExplainComplianceCaseDecisionResponse explainComplianceCaseDecision(ExplainComplianceCaseDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainComplianceCaseDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = (CompareChatRoomOptionsRequest.newBuilder()
                .setChatRoom(ChatRoom.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getComplianceCase().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getComplianceCase().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getComplianceCase().getAssignee().getDisplayName())
                                .setEmail(request.getComplianceCase().getAssignee().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply0 = chatChatRoomCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareChatRoomOptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainComplianceCaseDecision", request.getComplianceCase().getComplianceCaseId());
        return ExplainComplianceCaseDecisionResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getChatRoom().getOwner().getEmail())
                                .build())
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setStatus(request.getComplianceCase().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lints a compliance case.
     */
    public LintComplianceCasesResponse lintComplianceCases(LintComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var builder0 = BrowseBankAccountsRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setFilter(BrowseBankAccountsRequest.Filter.newBuilder()
                .setCreatedAfter(request.getComplianceCase().getAudit().getUpdatedAt())
                .putAllLabels(request.getAnnotationsMap())
                .setOwnerId(request.getComplianceCase().getComplianceCaseId())
                .build());
        var outbound0 = builder0.build();
        var reply0 = bankingBankAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseBankAccounts(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintComplianceCases", request.getComplianceCase().getSubject());
        return LintComplianceCasesResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setStatus(request.getComplianceCase().getStatus())
                        .build())
                .setOutcome(LintComplianceCasesResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a compliance case.
     */
    public GetComplianceCaseRulesResponse getComplianceCaseRules(GetComplianceCaseRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetComplianceCaseRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getComplianceCaseId(), "GetComplianceCaseRules");
        return GetComplianceCaseRulesResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .build())
                .build();
    }
}
