package com.acme.finance.domains.insurance;

import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentLine;
import com.acme.documents.v1.DocumentLineTax;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.featureflags.v1.ExplainFeatureFlagDecisionRequest;
import com.acme.featureflags.v1.ExplainFeatureFlagDecisionResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UpdateUserRequest;
import com.acme.identity.v1.UpdateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.ComparePolicyOptionsRequest;
import com.acme.insurance.v1.ComparePolicyOptionsResponse;
import com.acme.insurance.v1.EstimatePolicyRequest;
import com.acme.insurance.v1.EstimatePolicyResponse;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.GetPolicyRatesRequest;
import com.acme.insurance.v1.GetPolicyRatesResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.insurance.v1.RecalculatePoliciesRequest;
import com.acme.insurance.v1.RecalculatePoliciesResponse;
import com.acme.insurance.v1.SimulatePolicyRequest;
import com.acme.insurance.v1.SimulatePolicyResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsResponse;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersRequest;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersResponse;
import com.acme.ledger.v1.NotifyPostingBatchWatchersRequest;
import com.acme.ledger.v1.NotifyPostingBatchWatchersResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchLine;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.PreviewPostingBatchRequest;
import com.acme.ledger.v1.PreviewPostingBatchResponse;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesResponse;
import com.acme.ledger.v1.UnsubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.UnsubscribePostingBatchUpdatesResponse;
import com.acme.messaging.v1.BatchGetConversationsRequest;
import com.acme.messaging.v1.BatchGetConversationsResponse;
import com.acme.messaging.v1.ConversationServiceGrpc;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.UpdateMetricSeriesRequest;
import com.acme.metrics.v1.UpdateMetricSeriesResponse;
import com.acme.prescriptions.v1.Prescription;
import com.acme.prescriptions.v1.PrescriptionCalculationServiceGrpc;
import com.acme.prescriptions.v1.PrescriptionLine;
import com.acme.prescriptions.v1.PrescriptionLineTax;
import com.acme.prescriptions.v1.QuotePrescriptionRequest;
import com.acme.prescriptions.v1.QuotePrescriptionResponse;
import com.acme.recruiting.v1.CountJobOpeningsRequest;
import com.acme.recruiting.v1.CountJobOpeningsResponse;
import com.acme.recruiting.v1.JobOpeningQueryServiceGrpc;
import com.acme.search.v1.BatchGetSearchIndexesRequest;
import com.acme.search.v1.BatchGetSearchIndexesResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import com.acme.secrets.v1.SearchSecretsRequest;
import com.acme.secrets.v1.SearchSecretsResponse;
import com.acme.secrets.v1.SecretServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
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
 * Computes the figures of a policy, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PolicyCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(PolicyCalculationApplication.class);

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final ConversationServiceGrpc.ConversationServiceBlockingStub messagingConversationServiceBlocking;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final PrescriptionCalculationServiceGrpc.PrescriptionCalculationServiceBlockingStub prescriptionsPrescriptionCalculationServiceBlocking;

    private final JobOpeningQueryServiceGrpc.JobOpeningQueryServiceBlockingStub recruitingJobOpeningQueryServiceBlocking;

    private final InsuranceRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    private final SecretServiceGrpc.SecretServiceBlockingStub secretsSecretServiceBlocking;

    public PolicyCalculationApplication(
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            ConversationServiceGrpc.ConversationServiceBlockingStub messagingConversationServiceBlocking,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            PrescriptionCalculationServiceGrpc.PrescriptionCalculationServiceBlockingStub prescriptionsPrescriptionCalculationServiceBlocking,
            JobOpeningQueryServiceGrpc.JobOpeningQueryServiceBlockingStub recruitingJobOpeningQueryServiceBlocking,
            InsuranceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking,
            SecretServiceGrpc.SecretServiceBlockingStub secretsSecretServiceBlocking) {
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.messagingConversationServiceBlocking = messagingConversationServiceBlocking;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.prescriptionsPrescriptionCalculationServiceBlocking = prescriptionsPrescriptionCalculationServiceBlocking;
        this.recruitingJobOpeningQueryServiceBlocking = recruitingJobOpeningQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
        this.secretsSecretServiceBlocking = secretsSecretServiceBlocking;
    }

    /**
     * Calculates a policy.
     */
    public CalculatePolicyResponse calculatePolicy(CalculatePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculatePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var results0 = new ArrayList<QuotePrescriptionResponse>();
        for (var element : request.getPolicy().getLimitsList()) {
            QuotePrescriptionRequest itemRequest = QuotePrescriptionRequest.newBuilder()
                .setContext(request.getContext())
                .setPrescription(Prescription.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .addLines(PrescriptionLine.newBuilder()
                                .setTax(PrescriptionLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
            results0.add(prescriptionsPrescriptionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePrescription(itemRequest));
        }
        var reply0 = results0.isEmpty() ? QuotePrescriptionResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = BatchGetSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.retryRead(() -> searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetSearchIndexes(outbound1));

        var results2 = new ArrayList<UpdateMetricSeriesResponse>();
        for (var element : request.getPolicy().getLimitsList()) {
            UpdateMetricSeriesRequest itemRequest = UpdateMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .setName(element.getName())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getPrescription().getCounterparty().getDisplayName())
                                .setEmail(request.getPolicy().getContact().getEmail())
                                .build())
                        .build())
                .build();
            results2.add(metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateMetricSeries(itemRequest));
        }
        var reply2 = results2.isEmpty() ? UpdateMetricSeriesResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculatePolicy", request.getPolicy().getPolicyId());
        return CalculatePolicyResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setContact(request.getPolicy().getContact())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Estimates a policy.
     */
    public EstimatePolicyResponse estimatePolicy(EstimatePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimatePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = SubscribeFiscalPeriodUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeFiscalPeriodUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getPolicy().getContact().getEmail())
                                .setPhone(request.getPolicy().getContact().getPhone())
                                .setPreferredChannel(request.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<SubscribeFiscalPeriodUpdatesRequest, SubscribeFiscalPeriodUpdatesResponse>() {
            @Override
            public SubscribeFiscalPeriodUpdatesResponse apply(SubscribeFiscalPeriodUpdatesRequest input) {
                return ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeFiscalPeriodUpdates(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimatePolicy", request.getPolicy().getPolicyId());
        return EstimatePolicyResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getFiscalPeriod().getAudit().getCreatedBy())
                                .build())
                        .setAvailable(request.getPolicy().getAvailable())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getFiscalPeriod().getOrganizer().getEmail())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .build();
    }

    /**
     * Quotes a policy.
     */
    public QuotePolicyResponse quotePolicy(QuotePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuotePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var futures0 = request.getPolicy().getLimitsList().stream()
                .map(element -> PreviewPostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setPostingBatch(PostingBatch.newBuilder()
                        .addLines(PostingBatchLine.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPostingBatch(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? PreviewPostingBatchResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = UpdateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPostingBatch().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getPostingBatch().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getPolicy().getContact().getPhone())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getPostingBatch().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            UpdateUserResponse fetch(UpdateUserRequest input) {
                return identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateUser(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = BatchGetConversationsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(messagingConversationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetConversations(value)))
                .collect(Collectors.toList()).getFirst();

        var futures3 = request.getPolicy().getLimitsList().stream()
                .map(element -> UpdateSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setName(element.getName())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateSearchIndex(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? UpdateSearchIndexResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuotePolicy", request.getPolicy().getPolicyId());
        return QuotePolicyResponse.newBuilder()
                .setBreakdown(QuotePolicyResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getPolicy().getCurrencyCode())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getUser().getAudit().getEtag())
                                .build())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Compares a policy.
     */
    public ComparePolicyOptionsResponse comparePolicyOptions(ComparePolicyOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ComparePolicyOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        List<PreviewDocumentResponse> results0 = request.getPolicy().getLimitsList().stream()
                .map(element -> PreviewDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewDocumentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getPolicy().getContact().getEmail())
                                .setPhone(request.getPolicy().getContact().getPhone())
                                .build())
                        .build())
                .setDocument(Document.newBuilder()
                        .addLines(DocumentLine.newBuilder()
                                .setTax(DocumentLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewDocument(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? PreviewDocumentResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = SearchSecretsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchSecretsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getDocument().getAudit().getCreatedAt())
                        .putAllLabels(request.getPolicy().getLabelsMap())
                        .setRegion(reply0.getDocument().getBillingAddress().getRegion())
                        .build())
                .build();
        Function<SearchSecretsRequest, SearchSecretsResponse> operation1 = secretsSecretServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::searchSecrets;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = ListRetentionPolicyVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRetentionPolicyVersionsRequest.Filter.newBuilder()
                        .putAllLabels(request.getPolicy().getLabelsMap())
                        .setRegion(reply0.getDocument().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listRetentionPolicyVersions(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = RevertFiscalPeriodRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertFiscalPeriodRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build();
        RpcPort<RevertFiscalPeriodRequest, RevertFiscalPeriodResponse> port3 = RpcPort.adapt(ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::revertFiscalPeriod);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ComparePolicyOptions", request.getPolicy().getPolicyId());
        return ComparePolicyOptionsResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply3.getFiscalPeriod().getAudit().getCreatedBy())
                                .build())
                        .setContact(request.getPolicy().getContact())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Recalculates a policy.
     */
    public RecalculatePoliciesResponse recalculatePolicies(RecalculatePoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculatePolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = ListFiscalPeriodSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFiscalPeriodSubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPolicy().getAudit().getUpdatedAt())
                        .putAllLabels(request.getPolicy().getLabelsMap())
                        .setOwnerId(request.getPolicy().getHolder().getCoverageId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        Function<ListFiscalPeriodSubscriptionsRequest, ListFiscalPeriodSubscriptionsResponse> operation0 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listFiscalPeriodSubscriptions;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculatePolicies", request.getPolicy().getPolicyId());
        return RecalculatePoliciesResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setContact(request.getPolicy().getContact())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Simulates a policy.
     */
    public SimulatePolicyResponse simulatePolicy(SimulatePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulatePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = NotifyPostingBatchWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyPostingBatchWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getPolicy().getContact().getEmail())
                                .setName(request.getPolicy().getContact().getName())
                                .setPhone(request.getPolicy().getContact().getPhone())
                                .setPreferredChannel(request.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyPostingBatchWatchers(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = BeanCopies.copy(ExplainFeatureFlagDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainFeatureFlagDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setVersion(request.getPolicy().getHolder().getVersion())
                        .build())
                .build(), ExplainFeatureFlagDecisionRequest.class);
        var reply1 = featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainFeatureFlagDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulatePolicy", request.getPolicy().getPolicyId());
        return SimulatePolicyResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getFeatureFlag().getAudit().getUpdatedBy())
                                .build())
                        .setBalance(request.getPolicy().getBalance())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a policy.
     */
    public ExplainPolicyCalculationResponse explainPolicyCalculation(ExplainPolicyCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainPolicyCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePolicy(request.getContext().getTenantId(), request.getPolicy());

        var outbound0 = CountJobOpeningsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountJobOpeningsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPolicy().getAudit().getUpdatedAt())
                        .putAllLabels(request.getPolicy().getLabelsMap())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(recruitingJobOpeningQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countJobOpenings(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = NotifyFiscalPeriodWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyFiscalPeriodWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setName(request.getPolicy().getContact().getName())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyFiscalPeriodWatchers(value));
        log.atDebug().addKeyValue("auditValue", request.getPolicy().getPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainPolicyCalculation", request.getPolicy().getPolicyId());
        return ExplainPolicyCalculationResponse.newBuilder()
                .setOutcome(ExplainPolicyCalculationResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setCurrencyCode(request.getPolicy().getCurrencyCode())
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a policy.
     */
    public GetPolicyRatesResponse getPolicyRates(GetPolicyRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPolicyRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Policy", request.getPolicyId(), "GetPolicyRates");

        var builder0 = UnsubscribePostingBatchUpdatesRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(UnsubscribePostingBatchUpdatesRequest.Details.newBuilder()
                .setPayload(Any.pack(request.getContext()))
                .setRecipient(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .putAllVariables(request.getContext().getBaggageMap())
                .build());
        var outbound0 = builder0.build();
        var reply0 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribePostingBatchUpdates(outbound0);
        return GetPolicyRatesResponse.newBuilder()
                .setPolicy(Policy.newBuilder()
                        .setPolicyId(request.getPolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }
}
