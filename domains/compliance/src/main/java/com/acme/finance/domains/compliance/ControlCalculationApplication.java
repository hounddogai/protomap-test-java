package com.acme.finance.domains.compliance;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationRef;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CalculateControlRequest;
import com.acme.compliance.v1.CalculateControlResponse;
import com.acme.compliance.v1.CompareControlOptionsRequest;
import com.acme.compliance.v1.CompareControlOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.EstimateComplianceCaseRequest;
import com.acme.compliance.v1.EstimateComplianceCaseResponse;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.ExplainControlCalculationRequest;
import com.acme.compliance.v1.ExplainControlCalculationResponse;
import com.acme.compliance.v1.GetControlRatesRequest;
import com.acme.compliance.v1.GetControlRatesResponse;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.compliance.v1.QuoteControlRequest;
import com.acme.compliance.v1.QuoteControlResponse;
import com.acme.compliance.v1.RecalculateComplianceCasesRequest;
import com.acme.compliance.v1.RecalculateComplianceCasesResponse;
import com.acme.compliance.v1.RecalculateControlsRequest;
import com.acme.compliance.v1.RecalculateControlsResponse;
import com.acme.compliance.v1.SimulateControlRequest;
import com.acme.compliance.v1.SimulateControlResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CheckUnderwritingEligibilityRequest;
import com.acme.insurance.v1.CheckUnderwritingEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.CompareCoverageOptionsRequest;
import com.acme.insurance.v1.CompareCoverageOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.insurance.v1.ValidateUnderwritingRequest;
import com.acme.insurance.v1.ValidateUnderwritingResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.RecordLedgerAccountVersionRequest;
import com.acme.ledger.v1.RecordLedgerAccountVersionResponse;
import com.acme.lending.v1.BrowseLoanApplicationsRequest;
import com.acme.lending.v1.BrowseLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.risk.v1.EstimateExposureRequest;
import com.acme.risk.v1.EstimateExposureResponse;
import com.acme.risk.v1.Exposure;
import com.acme.telematics.v1.CalculateTelemetryReadingRequest;
import com.acme.telematics.v1.CalculateTelemetryReadingResponse;
import com.acme.telematics.v1.TelemetryReading;
import com.acme.telematics.v1.TelemetryReadingCalculationServiceGrpc;
import com.acme.venues.v1.CountVenuesRequest;
import com.acme.venues.v1.CountVenuesResponse;
import com.acme.venues.v1.SuggestVenuesRequest;
import com.acme.venues.v1.SuggestVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a control, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class ControlCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(ControlCalculationApplication.class);

    private final ChannelRegistry channels;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceFutureStub complianceComplianceCaseCalculationServiceFuture;

    private final ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking;

    private final CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking;

    private final UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking;

    private final ComplianceRepository repository;

    private final Executor rpcExecutor;

    private final TelemetryReadingCalculationServiceGrpc.TelemetryReadingCalculationServiceBlockingStub telematicsTelemetryReadingCalculationServiceBlocking;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public ControlCalculationApplication(
            ChannelRegistry channels,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceFutureStub complianceComplianceCaseCalculationServiceFuture,
            ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking,
            CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking,
            UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            LoanApplicationQueryServiceGrpc.LoanApplicationQueryServiceBlockingStub lendingLoanApplicationQueryServiceBlocking,
            ComplianceRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            TelemetryReadingCalculationServiceGrpc.TelemetryReadingCalculationServiceBlockingStub telematicsTelemetryReadingCalculationServiceBlocking,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.channels = channels;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.complianceComplianceCaseCalculationServiceFuture = complianceComplianceCaseCalculationServiceFuture;
        this.insuranceClaimCalculationServiceBlocking = insuranceClaimCalculationServiceBlocking;
        this.insuranceCoverageCalculationServiceBlocking = insuranceCoverageCalculationServiceBlocking;
        this.insuranceUnderwritingValidationServiceBlocking = insuranceUnderwritingValidationServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.lendingLoanApplicationQueryServiceBlocking = lendingLoanApplicationQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.telematicsTelemetryReadingCalculationServiceBlocking = telematicsTelemetryReadingCalculationServiceBlocking;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    /**
     * Calculates a control.
     */
    public CalculateControlResponse calculateControl(CalculateControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = BeanCopies.copy(CompareCoverageOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getControl().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getControl().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setDetails(CompareCoverageOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build(), CompareCoverageOptionsRequest.class);
        var reply0 = insuranceCoverageCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCoverageOptions(outbound0);

        var outbound1 = QuoteComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getControl().getOwner().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getControl().getAudit().getCreatedBy())
                                .setEtag(request.getControl().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = complianceComplianceCaseCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).quoteComplianceCase(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateControl", request.getControl().getControlId());
        return CalculateControlResponse.newBuilder()
                .setBreakdown(CalculateControlResponse.Breakdown.newBuilder()
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getCoverage().getOwner().getDisplayName())
                                .build())
                        .setVersion(request.getControl().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Estimates a control.
     */
    public EstimateControlResponse estimateControl(EstimateControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = (EstimateComplianceCaseRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateComplianceCaseRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateComplianceCase(outbound0);

        var outbound1 = CalculateTelemetryReadingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateTelemetryReadingRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .setTelemetryReading(TelemetryReading.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> telematicsTelemetryReadingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateTelemetryReading(value));
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateControl", request.getControl().getControlId());
        return EstimateControlResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setEffective(request.getControl().getEffective())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Quotes a control.
     */
    public QuoteControlResponse quoteControl(QuoteControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = ExplainComplianceCaseCalculationRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getControl().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainComplianceCaseCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getControl().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = complianceComplianceCaseCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).explainComplianceCaseCalculation(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = RecalculateComplianceCasesRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getControl().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getComplianceCase().getAudit().getUpdatedBy())
                                .build())
                        .setCustomer(AttestationRef.newBuilder()
                                .setNumber(switch (reply0.getComplianceCase().getOriginCase()) {
                                    case CUSTOMER -> reply0.getComplianceCase().getCustomer().getNumber();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateComplianceCasesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply1 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateComplianceCases(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteControl", request.getControl().getControlId());
        return QuoteControlResponse.newBuilder()
                .setBreakdown(QuoteControlResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getComplianceCase().getRequester().getEmail())
                                .build())
                        .setPriority(reply1.getComplianceCase().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Compares a control.
     */
    public CompareControlOptionsResponse compareControlOptions(CompareControlOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareControlOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = CountVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getControl().getAudit().getCreatedAt())
                        .setOwnerId(request.getControl().getOwner().getUserId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countVenues(value)).orElseThrow();

        var outbound1 = ValidateUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getControl().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateUnderwriting(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareControlOptions", request.getControl().getControlId());
        return CompareControlOptionsResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setEffective(request.getControl().getEffective())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalculates a control.
     */
    public RecalculateControlsResponse recalculateControls(RecalculateControlsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateControls")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = SuggestVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getControl().getAudit().getUpdatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestVenues(outbound0), rpcExecutor);

        var outbound1 = BrowseLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLoanApplicationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getControl().getAudit().getCreatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> lendingLoanApplicationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseLoanApplications(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateControls", request.getControl().getControlId());
        return RecalculateControlsResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setEffective(request.getControl().getEffective())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Simulates a control.
     */
    public SimulateControlResponse simulateControl(SimulateControlRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateControl")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = QuoteComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setCustomer(AttestationRef.newBuilder()
                                .setDisplayName(request.getControl().getOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteComplianceCaseRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .build())
                .build();
        var reply0 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteComplianceCase(outbound0);

        var outbound1 = RecordLedgerAccountVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordLedgerAccountVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getControl().getAudit().getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getComplianceCase().getRequester().getEmail())
                                .build())
                        .setHolder(PostingBatchRef.newBuilder()
                                .setDisplayName(request.getControl().getOwner().getDisplayName())
                                .setNumber(switch (reply0.getComplianceCase().getOriginCase()) {
                                    case CUSTOMER -> reply0.getComplianceCase().getCustomer().getNumber();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordLedgerAccountVersion(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateControl", request.getControl().getControlId());
        return SimulateControlResponse.newBuilder()
                .setBreakdown(SimulateControlResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply1.getLedgerAccount().getCurrencyCode())
                        .build())
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getComplianceCase().getRequester().getEmail())
                                .build())
                        .setStatus(request.getControl().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a control.
     */
    public ExplainControlCalculationResponse explainControlCalculation(ExplainControlCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainControlCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveControl(request.getContext().getTenantId(), request.getControl());

        var outbound0 = CalculateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getControl().getOwner().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateComplianceCase(outbound0);

        var builder1 = CheckUnderwritingEligibilityRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setDetails(CheckUnderwritingEligibilityRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setPhone(request.getDetails().getContact().getPhone())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = insuranceUnderwritingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkUnderwritingEligibility(outbound1);

        var outbound2 = EstimateExposureRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getUnderwriting().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.risk.v1.ExposureCalculationService");
        var serviceName2 = "acme.risk.v1" + "." + "ExposureCalculationService";
        var methodName2 = "Est" + "imateExposure";
        var descriptor2 = MethodDescriptor.<EstimateExposureRequest, EstimateExposureResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(EstimateExposureRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(EstimateExposureResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getControl().getControlId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainControlCalculation", request.getControl().getControlId());
        return ExplainControlCalculationResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setDescription(request.getControl().getDescription())
                        .build())
                .setOutcome(ExplainControlCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a control.
     */
    public GetControlRatesResponse getControlRates(GetControlRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetControlRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Control", request.getControlId(), "GetControlRates");

        var outbound0 = ExplainClaimCalculationRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setSubject(request.getControlId())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainClaimCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var adapter0 = new Function<ExplainClaimCalculationRequest, ExplainClaimCalculationResponse>() {
            @Override
            public ExplainClaimCalculationResponse apply(ExplainClaimCalculationRequest input) {
                return insuranceClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainClaimCalculation(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        return GetControlRatesResponse.newBuilder()
                .setControl(Control.newBuilder()
                        .setControlId(request.getControlId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getClaim().getAssignee().getEmail())
                                .setUserId(reply0.getClaim().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
    }
}
