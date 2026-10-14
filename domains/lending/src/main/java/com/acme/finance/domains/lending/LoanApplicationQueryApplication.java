package com.acme.finance.domains.lending;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.CompareClaimOptionsRequest;
import com.acme.insurance.v1.CompareClaimOptionsResponse;
import com.acme.insurance.v1.CompareUnderwritingOptionsRequest;
import com.acme.insurance.v1.CompareUnderwritingOptionsResponse;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.lending.v1.BrowseLoanApplicationsRequest;
import com.acme.lending.v1.BrowseLoanApplicationsResponse;
import com.acme.lending.v1.CalculateLoanApplicationRequest;
import com.acme.lending.v1.CalculateLoanApplicationResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CompareCollateralOptionsRequest;
import com.acme.lending.v1.CompareCollateralOptionsResponse;
import com.acme.lending.v1.CountLoanApplicationsRequest;
import com.acme.lending.v1.CountLoanApplicationsResponse;
import com.acme.lending.v1.ExportLoanApplicationsRequest;
import com.acme.lending.v1.ExportLoanApplicationsResponse;
import com.acme.lending.v1.LintLoanApplicationsRequest;
import com.acme.lending.v1.LintLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LookupLoanApplicationRequest;
import com.acme.lending.v1.LookupLoanApplicationResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.ResolveLoanApplicationsRequest;
import com.acme.lending.v1.ResolveLoanApplicationsResponse;
import com.acme.lending.v1.ScoreRepaymentRequest;
import com.acme.lending.v1.ScoreRepaymentResponse;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.acme.lending.v1.SuggestLoanApplicationsRequest;
import com.acme.lending.v1.SuggestLoanApplicationsResponse;
import com.acme.lending.v1.SummarizeLoanApplicationsRequest;
import com.acme.lending.v1.SummarizeLoanApplicationsResponse;
import com.acme.maintenance.v1.FindWorkOrdersRequest;
import com.acme.maintenance.v1.FindWorkOrdersResponse;
import com.acme.maintenance.v1.WorkOrderQueryServiceGrpc;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about loan applications, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class LoanApplicationQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(LoanApplicationQueryApplication.class);

    private final ChannelRegistry channels;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking;

    private final CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking;

    private final LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking;

    private final RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking;

    private final WorkOrderQueryServiceGrpc.WorkOrderQueryServiceBlockingStub maintenanceWorkOrderQueryServiceBlocking;

    private final LendingRepository repository;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    public LoanApplicationQueryApplication(
            ChannelRegistry channels,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking,
            CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking,
            LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking,
            RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking,
            WorkOrderQueryServiceGrpc.WorkOrderQueryServiceBlockingStub maintenanceWorkOrderQueryServiceBlocking,
            LendingRepository repository,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking) {
        this.channels = channels;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.insuranceUnderwritingCalculationServiceBlocking = insuranceUnderwritingCalculationServiceBlocking;
        this.lendingCollateralCalculationServiceBlocking = lendingCollateralCalculationServiceBlocking;
        this.lendingLoanApplicationCalculationServiceBlocking = lendingLoanApplicationCalculationServiceBlocking;
        this.lendingRepaymentValidationServiceBlocking = lendingRepaymentValidationServiceBlocking;
        this.maintenanceWorkOrderQueryServiceBlocking = maintenanceWorkOrderQueryServiceBlocking;
        this.repository = repository;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
    }

    /**
     * Returns a loan application.
     */
    public LookupLoanApplicationResponse lookupLoanApplication(LookupLoanApplicationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupLoanApplication")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getLoanApplicationId(), "LookupLoanApplication");

        var outbound0 = SimulateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command0 = new DispatchCommand<>("acme.lending.v1.CollateralCalculationService/SimulateCollateral", outbound0);
        var reply0 = lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCollateral(command0.payload());
        return LookupLoanApplicationResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getCollateral().getAddress().getRegion())
                                .build())
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists loan applications a page at a time.
     */
    public BrowseLoanApplicationsResponse browseLoanApplications(BrowseLoanApplicationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseLoanApplications")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getContext().getTraceId(), "BrowseLoanApplications");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseLoanApplications", request.getPage().getPageToken());
        return BrowseLoanApplicationsResponse.getDefaultInstance();
    }

    /**
     * Counts loan applications by status.
     */
    public CountLoanApplicationsResponse countLoanApplications(CountLoanApplicationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountLoanApplications")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getContext().getTraceId(), "CountLoanApplications");

        var outbound0 = BrowseSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSearchIndexesRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSearchIndexes(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountLoanApplications", request.getFilter().getQuery());
        return CountLoanApplicationsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns several loan applications at once.
     */
    public ResolveLoanApplicationsResponse resolveLoanApplications(ResolveLoanApplicationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveLoanApplications")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getContext().getTraceId(), "ResolveLoanApplications");

        var outbound0 = FindWorkOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindWorkOrdersRequest.Filter.newBuilder()
                        .putAllLabels(request.getContext().getBaggageMap())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> maintenanceWorkOrderQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findWorkOrders(outbound0));
        return ResolveLoanApplicationsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams every matching loan application.
     */
    public ExportLoanApplicationsResponse exportLoanApplications(ExportLoanApplicationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportLoanApplications")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getContext().getTraceId(), "ExportLoanApplications");

        var outbound0 = CompareUnderwritingOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareUnderwritingOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<CompareUnderwritingOptionsRequest, CompareUnderwritingOptionsResponse>() {
            @Override
            public CompareUnderwritingOptionsResponse apply(CompareUnderwritingOptionsRequest input) {
                return insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareUnderwritingOptions(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = BeanCopies.copy(CalculateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getUnderwriting().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getUnderwriting().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build(), CalculateLoanApplicationRequest.class);
        var reply1 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateLoanApplication(outbound1);

        var outbound2 = CompareCollateralOptionsRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getLoanApplication().getBillingAddress().getPostalCode())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<CompareCollateralOptionsRequest, CompareCollateralOptionsResponse> operation2 = lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::compareCollateralOptions;
        var reply2 = operation2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportLoanApplications", request.getFilter().getQuery());
        return ExportLoanApplicationsResponse.newBuilder()
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Summarizes a loan application.
     */
    public SummarizeLoanApplicationsResponse summarizeLoanApplications(SummarizeLoanApplicationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeLoanApplications")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getLoanApplicationId(), "SummarizeLoanApplications");

        var outbound0 = LintLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setLoanApplicationId(request.getLoanApplicationId())
                        .build())
                .setReason(request.getReason())
                .build();
        var channel0 = channels.lookup("acme.lending.v1.LoanApplicationValidationService");
        var serviceName0 = "acme.lending.v1" + "." + "LoanApplicationValidationService";
        var methodName0 = "Lin" + "tLoanApplications";
        var descriptor0 = MethodDescriptor.<LintLoanApplicationsRequest, LintLoanApplicationsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(LintLoanApplicationsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(LintLoanApplicationsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ScoreRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getLoanApplication().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> lendingRepaymentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRepayment(value));
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeLoanApplications", request.getDetails().getFormat());
        return SummarizeLoanApplicationsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists loan applications a page at a time.
     */
    public SuggestLoanApplicationsResponse suggestLoanApplications(SuggestLoanApplicationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestLoanApplications")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LoanApplication", request.getContext().getTraceId(), "SuggestLoanApplications");

        var outbound0 = CompareClaimOptionsRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareClaimOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.insurance.v1.ClaimCalculationService/CompareClaimOptions", outbound0);
        var reply0 = insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareClaimOptions(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestLoanApplications", request.getPage().getPageToken());
        return SuggestLoanApplicationsResponse.newBuilder()
                .addLoanApplications(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getClaim().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
    }
}
