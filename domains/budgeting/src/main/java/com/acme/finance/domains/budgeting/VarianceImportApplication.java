package com.acme.finance.domains.budgeting;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.ListAuditEventSubscriptionsRequest;
import com.acme.audit.v1.ListAuditEventSubscriptionsResponse;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.PreviewAccessPolicyRequest;
import com.acme.authorization.v1.PreviewAccessPolicyResponse;
import com.acme.budgeting.v1.CancelVarianceImportRequest;
import com.acme.budgeting.v1.CancelVarianceImportResponse;
import com.acme.budgeting.v1.EstimateForecastRequest;
import com.acme.budgeting.v1.EstimateForecastResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.GetVarianceImportStatusRequest;
import com.acme.budgeting.v1.GetVarianceImportStatusResponse;
import com.acme.budgeting.v1.ImportVariancesRequest;
import com.acme.budgeting.v1.ImportVariancesResponse;
import com.acme.budgeting.v1.ListVarianceImportsRequest;
import com.acme.budgeting.v1.ListVarianceImportsResponse;
import com.acme.budgeting.v1.MapVarianceImportColumnsRequest;
import com.acme.budgeting.v1.MapVarianceImportColumnsResponse;
import com.acme.budgeting.v1.RecalculateForecastsRequest;
import com.acme.budgeting.v1.RecalculateForecastsResponse;
import com.acme.budgeting.v1.ResolveBudgetLinesRequest;
import com.acme.budgeting.v1.ResolveBudgetLinesResponse;
import com.acme.budgeting.v1.RetryVarianceImportRequest;
import com.acme.budgeting.v1.RetryVarianceImportResponse;
import com.acme.budgeting.v1.ValidateVarianceImportRequest;
import com.acme.budgeting.v1.ValidateVarianceImportResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.search.v1.ExplainSearchIndexDecisionRequest;
import com.acme.search.v1.ExplainSearchIndexDecisionResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.suppliers.v1.BrowseSuppliersRequest;
import com.acme.suppliers.v1.BrowseSuppliersResponse;
import com.acme.suppliers.v1.ExportSuppliersRequest;
import com.acme.suppliers.v1.ExportSuppliersResponse;
import com.acme.suppliers.v1.FindSupplierContractsRequest;
import com.acme.suppliers.v1.FindSupplierContractsResponse;
import com.acme.suppliers.v1.ResolveSuppliersRequest;
import com.acme.suppliers.v1.ResolveSuppliersResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Takes variances in bulk from partners and legacy systems. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class VarianceImportApplication {
    private static final Logger log = LoggerFactory.getLogger(VarianceImportApplication.class);

    private final AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final BudgetingRepository repository;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    public VarianceImportApplication(
            AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            BudgetingRepository repository,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking) {
        this.auditAuditEventNotificationServiceBlocking = auditAuditEventNotificationServiceBlocking;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.budgetingForecastCalculationServiceBlocking = budgetingForecastCalculationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.repository = repository;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
    }

    public record MapVarianceImportColumnsStage0Requested(ResolveBudgetLinesRequest request, CompletableFuture<ResolveBudgetLinesResponse> completion, io.grpc.Context context) {}

    /**
     * Takes a stream of variances and answers once at the end.
     */
    public ImportVariancesResponse importVariances(ImportVariancesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ImportVariances")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveVariance(request.getContext().getTenantId(), request.getRecord());
        }

        List<EstimateForecastResponse> results0 = request.getRecord().getCommentsList().stream()
                .map(element -> EstimateForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .setUpdatedBy(request.getRecord().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateForecast(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? EstimateForecastResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = PreviewAccessPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewAccessPolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getRequester().getEmail();
                                    default -> "";
                                })
                                .setPreferredChannel(switch (request.getChunkCase()) {
                                    case HEADER -> request.getHeader().getSubmittedBy().getPreferredChannel();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAccessPolicy(outbound1);

        var outbound2 = ResolveSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.suppliers.v1.SupplierQueryService");
        var selected2 = SupplierQueryServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.resolveSuppliers(outbound2);

        var outbound3 = ListAuditEventSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditEventSubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRecord().getSla().getDueAt())
                        .setRegion(reply0.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply3 = RpcSupport.retryRead(() -> auditAuditEventNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEventSubscriptions(outbound3));
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ImportVariances", request.getHeader().getSource());
        return ImportVariancesResponse.newBuilder()
                .addRejections(ImportVariancesResponse.Rejection.newBuilder()
                        .setReason(reply1.getAccessPolicy().getBlock().getReason())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a variance.
     */
    public GetVarianceImportStatusResponse getVarianceImportStatus(GetVarianceImportStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetVarianceImportStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "GetVarianceImportStatus");
        return GetVarianceImportStatusResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Cancels a variance.
     */
    public CancelVarianceImportResponse cancelVarianceImport(CancelVarianceImportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelVarianceImport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "CancelVarianceImport");

        var outbound0 = ExportSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var stream0 = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSuppliers(outbound0);
        var reply0 = ExportSuppliersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelVarianceImport", request.getDetails().getReasonCode());
        return CancelVarianceImportResponse.newBuilder()
                .setOutcome(CancelVarianceImportResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Validates a variance.
     */
    public ValidateVarianceImportResponse validateVarianceImport(ValidateVarianceImportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateVarianceImport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        List<FindSupplierContractsResponse> results0 = request.getVariance().getCommentsList().stream()
                .map(element -> FindSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindSupplierContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getCreatedAt())
                        .build())
                .build())
                .flatMap(value -> Stream.of(suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findSupplierContracts(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? FindSupplierContractsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = BrowseSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getVariance().getSla().getDueAt())
                        .setOwnerId(request.getVariance().getSla().getPolicyId())
                        .build())
                .build();
        final class Stage1Adapter {
            BrowseSuppliersResponse fetch(BrowseSuppliersRequest input) {
                return suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSuppliers(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateVarianceImport", request.getVariance().getVarianceId());
        return ValidateVarianceImportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(request.getVariance().getAssignee())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Lists variances a page at a time.
     */
    public ListVarianceImportsResponse listVarianceImports(ListVarianceImportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListVarianceImports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getContext().getTraceId(), "ListVarianceImports");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListVarianceImports", request.getPage().getPageToken());
        return ListVarianceImportsResponse.getDefaultInstance();
    }

    /**
     * Retrys a variance.
     */
    public RetryVarianceImportResponse retryVarianceImport(RetryVarianceImportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryVarianceImport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = ExplainSearchIndexDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainSearchIndexDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getVariance().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getVariance().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getVariance().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainSearchIndexDecisionRequest, ExplainSearchIndexDecisionResponse> port0 = RpcPort.adapt(searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainSearchIndexDecision);
        var reply0 = port0.exchange(outbound0);

        var results1 = new ArrayList<ExportSuppliersResponse>();
        for (var element : request.getVariance().getCommentsList()) {
            ExportSuppliersRequest itemRequest = ExportSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getCreatedAt())
                        .setRegion(reply0.getSearchIndex().getAddress().getRegion())
                        .build())
                .build();
            var responses = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSuppliers(itemRequest);
            responses.forEachRemaining(results1::add);
        }
        var reply1 = results1.isEmpty() ? ExportSuppliersResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryVarianceImport", request.getVariance().getVarianceId());
        return RetryVarianceImportResponse.newBuilder()
                .setOutcome(RetryVarianceImportResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setVariance(Variance.newBuilder()
                        .putAllCustomFields(request.getVariance().getCustomFieldsMap())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Maps a variance.
     */
    public MapVarianceImportColumnsResponse mapVarianceImportColumns(MapVarianceImportColumnsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "MapVarianceImportColumns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = ResolveBudgetLinesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<ResolveBudgetLinesResponse>();
        events.publishEvent(new MapVarianceImportColumnsStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        List<RecalculateForecastsResponse> results1 = request.getVariance().getCommentsList().stream()
                .map(element -> RecalculateForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateForecasts(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? RecalculateForecastsResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "MapVarianceImportColumns", request.getVariance().getVarianceId());
        return MapVarianceImportColumnsResponse.newBuilder()
                .setOutcome(MapVarianceImportColumnsResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setPriority(request.getVariance().getPriority())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }
}
