package com.acme.finance.domains.payroll;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.FindDocumentsRequest;
import com.acme.documents.v1.FindDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.ExportUsersRequest;
import com.acme.identity.v1.ExportUsersResponse;
import com.acme.identity.v1.FindUsersRequest;
import com.acme.identity.v1.FindUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.PreviewCoverageRequest;
import com.acme.insurance.v1.PreviewCoverageResponse;
import com.acme.payroll.v1.CalculatePayRunRequest;
import com.acme.payroll.v1.CalculatePayRunResponse;
import com.acme.payroll.v1.CalculatePayStubRequest;
import com.acme.payroll.v1.CalculatePayStubResponse;
import com.acme.payroll.v1.ComparePayRunOptionsRequest;
import com.acme.payroll.v1.ComparePayRunOptionsResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.ExplainPayRunCalculationRequest;
import com.acme.payroll.v1.ExplainPayRunCalculationResponse;
import com.acme.payroll.v1.ExplainPayStubCalculationRequest;
import com.acme.payroll.v1.ExplainPayStubCalculationResponse;
import com.acme.payroll.v1.GetPayStubRatesRequest;
import com.acme.payroll.v1.GetPayStubRatesResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunAttachment;
import com.acme.payroll.v1.PayRunBankSettlement;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunCardSettlement;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.QuotePayStubRequest;
import com.acme.payroll.v1.QuotePayStubResponse;
import com.acme.payroll.v1.RecalculatePayStubsRequest;
import com.acme.payroll.v1.RecalculatePayStubsResponse;
import com.acme.payroll.v1.SimulatePayRunRequest;
import com.acme.payroll.v1.SimulatePayRunResponse;
import com.acme.payroll.v1.SimulatePayStubRequest;
import com.acme.payroll.v1.SimulatePayStubResponse;
import com.acme.procurement.v1.CreatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CreatePurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionAttachment;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
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
 * Computes the figures of a pay stub, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PayStubCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(PayStubCalculationApplication.class);

    private final ChannelRegistry channels;

    private final DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking;

    private final PayrollRepository repository;

    private final Executor rpcExecutor;

    public PayStubCalculationApplication(
            ChannelRegistry channels,
            DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceBlockingStub procurementPurchaseRequisitionServiceBlocking,
            PayrollRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.channels = channels;
        this.documentsDocumentQueryServiceBlocking = documentsDocumentQueryServiceBlocking;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.payrollPayRunCalculationServiceBlocking = payrollPayRunCalculationServiceBlocking;
        this.procurementPurchaseRequisitionServiceBlocking = procurementPurchaseRequisitionServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Calculates a pay stub.
     */
    public CalculatePayStubResponse calculatePayStub(CalculatePayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculatePayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = SimulatePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulatePayRunRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getPayStub().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply0 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePayRun(outbound0);

        List<QuotePayRunResponse> results1 = request.getPayStub().getLinesList().stream()
                .map(element -> QuotePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .addLines(PayRunLine.newBuilder()
                                .setTax(PayRunLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePayRun(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? QuotePayRunResponse.getDefaultInstance()
                : results1.getLast();

        var results2 = new ArrayList<CreatePurchaseRequisitionResponse>();
        for (var element : request.getPayStub().getAttachmentsList()) {
            CreatePurchaseRequisitionRequest itemRequest = CreatePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .addAttachments(PurchaseRequisitionAttachment.newBuilder()
                                .setContentType(element.getContentType())
                                .setFileName(element.getFileName())
                                .setSizeBytes(element.getSizeBytes())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build();
            results2.add(procurementPurchaseRequisitionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createPurchaseRequisition(itemRequest));
        }
        var reply2 = results2.isEmpty() ? CreatePurchaseRequisitionResponse.getDefaultInstance()
                : results2.getLast();

        var futures3 = request.getPayStub().getAttachmentsList().stream()
                .map(element -> ExplainPayRunCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .addAttachments(PayRunAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPayRunCalculation(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? ExplainPayRunCalculationResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculatePayStub", request.getPayStub().getPayStubId());
        return CalculatePayStubResponse.newBuilder()
                .setBreakdown(CalculatePayStubResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPayRun().getAudit().getCreatedBy())
                                .setEtag(reply0.getPayRun().getAudit().getEtag())
                                .build())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Estimates a pay stub.
     */
    public EstimatePayStubResponse estimatePayStub(EstimatePayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimatePayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = CalculatePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculatePayRunRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPayStub().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .setCard(PayRunCardSettlement.newBuilder()
                                .setToken(request.getPayStub().getCard().getToken())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePayRun(outbound0), rpcExecutor);

        var outbound1 = PreviewCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPayStub().getAudit().getCreatedBy())
                                .setEtag(request.getPayStub().getAudit().getEtag())
                                .setUpdatedAt(request.getPayStub().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getPayStub().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewCoverage(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimatePayStub", request.getPayStub().getPayStubId());
        return EstimatePayStubResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getCoverage().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getPayRun().getBillingAddress().getPostalCode())
                                .build())
                        .putAllMetadata(request.getPayStub().getMetadataMap())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Quotes a pay stub.
     */
    public QuotePayStubResponse quotePayStub(QuotePayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuotePayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = FindDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPayStub().getIssuedAt())
                        .putAllLabels(request.getPayStub().getMetadataMap())
                        .setOwnerId(request.getPayStub().getCounterparty().getDeductionId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(documentsDocumentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findDocuments(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = ComparePayRunOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.payroll.v1.PayRunCalculationService");
        var serviceName1 = "acme.payroll.v1" + "." + "PayRunCalculationService";
        var methodName1 = "Com" + "parePayRunOptions";
        var descriptor1 = MethodDescriptor.<ComparePayRunOptionsRequest, ComparePayRunOptionsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ComparePayRunOptionsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ComparePayRunOptionsResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var futures2 = request.getPayStub().getLinesList().stream()
                .map(element -> CalculatePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .addLines(PayRunLine.newBuilder()
                                .setTax(PayRunLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePayRun(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? CalculatePayRunResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuotePayStub", request.getPayStub().getPayStubId());
        return QuotePayStubResponse.newBuilder()
                .setBreakdown(QuotePayStubResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply1.getBreakdown().getCurrencyCode())
                        .setTotal(request.getPayStub().getTotal())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getPayRun().getBillingAddress().getRegion())
                                .build())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Compares a pay stub.
     */
    public ComparePayStubOptionsResponse comparePayStubOptions(ComparePayStubOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ComparePayStubOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = BeanCopies.copy(ComparePayRunOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ComparePayRunOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getPayStub().getBillingAddress().getPostalCode())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getPayStub().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build(), ComparePayRunOptionsRequest.class);
        var reply0 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePayRunOptions(outbound0);

        var outbound1 = ExportUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPayStub().getAudit().getCreatedAt())
                        .setRegion(request.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportUsers(outbound1);
        var reply1 = ExportUsersResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ComparePayStubOptions", request.getPayStub().getPayStubId());
        return ComparePayStubOptionsResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .putAllMetadata(reply0.getPayRun().getMetadataMap())
                        .setPayStubId(request.getPayStubId())
                        .setTotal(request.getPayStub().getTotal())
                        .build())
                .build();
    }

    /**
     * Recalculates a pay stub.
     */
    public RecalculatePayStubsResponse recalculatePayStubs(RecalculatePayStubsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculatePayStubs")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = CalculatePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculatePayRunRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPayStub().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPayStub().getAudit().getCreatedBy())
                                .build())
                        .setBank(PayRunBankSettlement.newBuilder()
                                .setIban(switch (request.getPayStub().getSettlementCase()) {
                                    case BANK -> request.getPayStub().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePayRun(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculatePayStubs", request.getPayStub().getPayStubId());
        return RecalculatePayStubsResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDeductionId(reply0.getPayRun().getCounterparty().getDeductionId())
                                .build())
                        .setNumber(request.getPayStub().getNumber())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Simulates a pay stub.
     */
    public SimulatePayStubResponse simulatePayStub(SimulatePayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulatePayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var futures0 = request.getPayStub().getAttachmentsList().stream()
                .map(element -> ExplainPayRunCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .addAttachments(PayRunAttachment.newBuilder()
                                .setFileName(element.getFileName())
                                .setSizeBytes(element.getSizeBytes())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPayRunCalculation(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ExplainPayRunCalculationResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulatePayStub", request.getPayStub().getPayStubId());
        return SimulatePayStubResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setIssuedAt(request.getPayStub().getIssuedAt())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Explains a pay stub.
     */
    public ExplainPayStubCalculationResponse explainPayStubCalculation(ExplainPayStubCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainPayStubCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        List<FindUsersResponse> results0 = request.getPayStub().getLinesList().stream()
                .map(element -> FindUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindUsersRequest.Filter.newBuilder()
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build())
                .flatMap(value -> Stream.of(identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findUsers(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? FindUsersResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CalculatePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setCard(PayRunCardSettlement.newBuilder()
                                .setNetwork(request.getPayStub().getCard().getNetwork())
                                .build())
                        .build())
                .build();
        var reply1 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePayRun(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainPayStubCalculation", request.getPayStub().getPayStubId());
        return ExplainPayStubCalculationResponse.newBuilder()
                .setOutcome(ExplainPayStubCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(request.getPayStub().getBillingAddress())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Returns a pay stub.
     */
    public GetPayStubRatesResponse getPayStubRates(GetPayStubRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPayStubRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "GetPayStubRates");
        return GetPayStubRatesResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }
}
