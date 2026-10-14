package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.ListAuditEventVersionsRequest;
import com.acme.audit.v1.ListAuditEventVersionsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CompareComplianceCaseOptionsRequest;
import com.acme.compliance.v1.CompareComplianceCaseOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.EstimateComplianceCaseRequest;
import com.acme.compliance.v1.EstimateComplianceCaseResponse;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.GetComplianceCaseRatesRequest;
import com.acme.compliance.v1.GetComplianceCaseRatesResponse;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.compliance.v1.RecalculateComplianceCasesRequest;
import com.acme.compliance.v1.RecalculateComplianceCasesResponse;
import com.acme.compliance.v1.SimulateComplianceCaseRequest;
import com.acme.compliance.v1.SimulateComplianceCaseResponse;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.featureflags.v1.ListFeatureFlagsRequest;
import com.acme.featureflags.v1.ListFeatureFlagsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.ResolveUsersRequest;
import com.acme.identity.v1.ResolveUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.lending.v1.CalculateLoanRequest;
import com.acme.lending.v1.CalculateLoanResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.ExportLoansRequest;
import com.acme.lending.v1.ExportLoansResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.lending.v1.RecalculateLoansRequest;
import com.acme.lending.v1.RecalculateLoansResponse;
import com.acme.lineage.v1.LineageNodeQueryServiceGrpc;
import com.acme.lineage.v1.SuggestLineageNodesRequest;
import com.acme.lineage.v1.SuggestLineageNodesResponse;
import com.acme.media.v1.MediaAsset;
import com.acme.media.v1.MediaAssetCalculationServiceGrpc;
import com.acme.media.v1.RecalculateMediaAssetsRequest;
import com.acme.media.v1.RecalculateMediaAssetsResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.NormalizeMetricSeriesRequest;
import com.acme.metrics.v1.NormalizeMetricSeriesResponse;
import com.acme.notifications.v1.CloneNotificationRequest;
import com.acme.notifications.v1.CloneNotificationResponse;
import com.acme.notifications.v1.ListNotificationsRequest;
import com.acme.notifications.v1.ListNotificationsResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
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
 * Computes the figures of a compliance case, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ComplianceCaseCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(ComplianceCaseCalculationApplication.class);

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final ChannelRegistry channels;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking;

    private final LoanCalculationServiceGrpc.LoanCalculationServiceFutureStub lendingLoanCalculationServiceFuture;

    private final LoanQueryServiceGrpc.LoanQueryServiceBlockingStub lendingLoanQueryServiceBlocking;

    private final LineageNodeQueryServiceGrpc.LineageNodeQueryServiceBlockingStub lineageLineageNodeQueryServiceBlocking;

    private final MediaAssetCalculationServiceGrpc.MediaAssetCalculationServiceBlockingStub mediaMediaAssetCalculationServiceBlocking;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final ComplianceRepository repository;

    private final Executor rpcExecutor;

    public ComplianceCaseCalculationApplication(
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            ChannelRegistry channels,
            FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking,
            LoanCalculationServiceGrpc.LoanCalculationServiceFutureStub lendingLoanCalculationServiceFuture,
            LoanQueryServiceGrpc.LoanQueryServiceBlockingStub lendingLoanQueryServiceBlocking,
            LineageNodeQueryServiceGrpc.LineageNodeQueryServiceBlockingStub lineageLineageNodeQueryServiceBlocking,
            MediaAssetCalculationServiceGrpc.MediaAssetCalculationServiceBlockingStub mediaMediaAssetCalculationServiceBlocking,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            ComplianceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.channels = channels;
        this.featureflagsFeatureFlagServiceBlocking = featureflagsFeatureFlagServiceBlocking;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.lendingLoanCalculationServiceBlocking = lendingLoanCalculationServiceBlocking;
        this.lendingLoanCalculationServiceFuture = lendingLoanCalculationServiceFuture;
        this.lendingLoanQueryServiceBlocking = lendingLoanQueryServiceBlocking;
        this.lineageLineageNodeQueryServiceBlocking = lineageLineageNodeQueryServiceBlocking;
        this.mediaMediaAssetCalculationServiceBlocking = mediaMediaAssetCalculationServiceBlocking;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Calculates a compliance case.
     */
    public CalculateComplianceCaseResponse calculateComplianceCase(CalculateComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = ExportLoansRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportLoansRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getComplianceCase().getAudit().getCreatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getComplianceCase().getAssignee().getUserId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var stream0 = lendingLoanQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportLoans(outbound0);
        var reply0 = ExportLoansResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = RecalculateLoansRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getComplianceCase().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var pending1 = lendingLoanCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateLoans(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = NormalizeMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeMetricSeriesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply1.getLoan().getContact().getName())
                                .setPreferredChannel(request.getComplianceCase().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getLoan().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var command2 = new DispatchCommand<>("acme.metrics.v1.MetricSeriesValidationService/NormalizeMetricSeries", outbound2);
        var reply2 = metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeMetricSeries(command2.payload());
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return CalculateComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply2.getMetricSeries().getOwner().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getLoan().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getLoan().getAudit().getCreatedBy())
                                .build())
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .putAllCustomFields(request.getComplianceCase().getCustomFieldsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Estimates a compliance case.
     */
    public EstimateComplianceCaseResponse estimateComplianceCase(EstimateComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = ResolveUsersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.identity.v1.UserQueryService");
        var serviceName0 = "acme.identity.v1" + "." + "UserQueryService";
        var methodName0 = "Res" + "olveUsers";
        var descriptor0 = MethodDescriptor.<ResolveUsersRequest, ResolveUsersResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ResolveUsersRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ResolveUsersResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return EstimateComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .putAllCustomFields(request.getComplianceCase().getCustomFieldsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Quotes a compliance case.
     */
    public QuoteComplianceCaseResponse quoteComplianceCase(QuoteComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = ListNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getComplianceCase().getAudit().getCreatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listNotifications(outbound0), rpcExecutor);

        var outbound1 = CloneNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getComplianceCase().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneNotification(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var results2 = new ArrayList<SuggestLineageNodesResponse>();
        for (var element : request.getComplianceCase().getHistoryList()) {
            SuggestLineageNodesRequest itemRequest = SuggestLineageNodesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestLineageNodesRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getChangedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
            results2.add(lineageLineageNodeQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestLineageNodes(itemRequest));
        }
        var reply2 = results2.isEmpty() ? SuggestLineageNodesResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return QuoteComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .putAllCustomFields(request.getComplianceCase().getCustomFieldsMap())
                        .setPriority(reply1.getNotification().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Compares a compliance case.
     */
    public CompareComplianceCaseOptionsResponse compareComplianceCaseOptions(CompareComplianceCaseOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareComplianceCaseOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = ListAuditEventVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventVersionsRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEventVersions(outbound0));
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareComplianceCaseOptions", request.getComplianceCase().getComplianceCaseId());
        return CompareComplianceCaseOptionsResponse.newBuilder()
                .setBreakdown(CompareComplianceCaseOptionsResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalculates a compliance case.
     */
    public RecalculateComplianceCasesResponse recalculateComplianceCases(RecalculateComplianceCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateComplianceCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var outbound0 = ExplainLoanCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getComplianceCase().getAudit().getCreatedAt())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setNumber(switch (request.getComplianceCase().getOriginCase()) {
                                    case CUSTOMER -> request.getComplianceCase().getCustomer().getNumber();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLoanCalculation(outbound0);

        var outbound1 = ResolveUsersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveUsers(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateComplianceCases", request.getComplianceCase().getComplianceCaseId());
        return RecalculateComplianceCasesResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getLoan().getAudit().getUpdatedBy())
                                .build())
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setRequester(request.getComplianceCase().getRequester())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Simulates a compliance case.
     */
    public SimulateComplianceCaseResponse simulateComplianceCase(SimulateComplianceCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateComplianceCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var builder0 = CheckPostingBatchEligibilityRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(CheckPostingBatchEligibilityRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setName(request.getComplianceCase().getRequester().getName())
                        .build())
                .build());
        builder0.setPostingBatch(PostingBatch.newBuilder()
                .setBillingAddress(PostalAddress.newBuilder()
                        .setLocality(request.getDetails().getDestination().getLocality())
                        .setRecipient(request.getDetails().getDestination().getRecipient())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPostingBatchEligibility(outbound0);

        var outbound1 = BeanCopies.copy(ListFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFeatureFlagsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getComplianceCase().getSla().getDueAt())
                        .setRegion(reply0.getPostingBatch().getBillingAddress().getRegion())
                        .build())
                .build(), ListFeatureFlagsRequest.class);
        var reply1 = featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listFeatureFlags(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateComplianceCase", request.getComplianceCase().getComplianceCaseId());
        return SimulateComplianceCaseResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setSubject(request.getComplianceCase().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a compliance case.
     */
    public ExplainComplianceCaseCalculationResponse explainComplianceCaseCalculation(ExplainComplianceCaseCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainComplianceCaseCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveComplianceCase(request.getContext().getTenantId(), request.getComplianceCase());

        var results0 = new ArrayList<CalculateLoanResponse>();
        for (var element : request.getComplianceCase().getCommentsList()) {
            CalculateLoanRequest itemRequest = CalculateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setNumber(request.getComplianceCase().getCustomer().getNumber())
                                .build())
                        .build())
                .build();
            results0.add(lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateLoan(itemRequest));
        }
        var reply0 = results0.isEmpty() ? CalculateLoanResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = JsonCodec.roundTrip(RecalculateMediaAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setMediaAsset(MediaAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLoan().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getLoan().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getComplianceCase().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build(), RecalculateMediaAssetsRequest.class);
        var reply1 = mediaMediaAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateMediaAssets(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getComplianceCase().getComplianceCaseId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainComplianceCaseCalculation", request.getComplianceCase().getComplianceCaseId());
        return ExplainComplianceCaseCalculationResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getMediaAsset().getAudit().getEtag())
                                .build())
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .setRequester(request.getComplianceCase().getRequester())
                        .build())
                .build();
    }

    /**
     * Returns a compliance case.
     */
    public GetComplianceCaseRatesResponse getComplianceCaseRates(GetComplianceCaseRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetComplianceCaseRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ComplianceCase", request.getComplianceCaseId(), "GetComplianceCaseRates");
        return GetComplianceCaseRatesResponse.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setComplianceCaseId(request.getComplianceCaseId())
                        .build())
                .build();
    }
}
