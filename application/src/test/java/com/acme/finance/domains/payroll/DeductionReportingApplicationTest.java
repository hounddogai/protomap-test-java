package com.acme.finance.domains.payroll;

import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastBankSettlement;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.PushForecastRequest;
import com.acme.budgeting.v1.PushForecastResponse;
import com.acme.budgeting.v1.StreamForecastChangesRequest;
import com.acme.budgeting.v1.StreamForecastChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.SignalAttestationWorkflowRequest;
import com.acme.compliance.v1.SignalAttestationWorkflowResponse;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractApprovalServiceGrpc;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.contracts.v1.RejectContractRequest;
import com.acme.contracts.v1.RejectContractResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.StreamUnderwritingMetricsRequest;
import com.acme.insurance.v1.StreamUnderwritingMetricsResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.ledger.v1.AnnotateJournalEntryVersionRequest;
import com.acme.ledger.v1.AnnotateJournalEntryVersionResponse;
import com.acme.ledger.v1.CompareJournalEntryVersionsRequest;
import com.acme.ledger.v1.CompareJournalEntryVersionsResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryBankSettlement;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsRequest;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsResponse;
import com.acme.payroll.v1.CalculatePayRunRequest;
import com.acme.payroll.v1.CalculatePayRunResponse;
import com.acme.payroll.v1.CountDeductionTotalsRequest;
import com.acme.payroll.v1.CountDeductionTotalsResponse;
import com.acme.payroll.v1.CountPayStubsRequest;
import com.acme.payroll.v1.CountPayStubsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionDiscountAction;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.DeleteDeductionReportRequest;
import com.acme.payroll.v1.DeleteDeductionReportResponse;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.FetchDeductionReportRequest;
import com.acme.payroll.v1.FetchDeductionReportResponse;
import com.acme.payroll.v1.GenerateDeductionReportRequest;
import com.acme.payroll.v1.GenerateDeductionReportResponse;
import com.acme.payroll.v1.GetDeductionRatesRequest;
import com.acme.payroll.v1.GetDeductionRatesResponse;
import com.acme.payroll.v1.ListDeductionReportsRequest;
import com.acme.payroll.v1.ListDeductionReportsResponse;
import com.acme.payroll.v1.NormalizeDeductionRequest;
import com.acme.payroll.v1.NormalizeDeductionResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.ResolvePayRunsRequest;
import com.acme.payroll.v1.ResolvePayRunsResponse;
import com.acme.payroll.v1.ScheduleDeductionReportRequest;
import com.acme.payroll.v1.ScheduleDeductionReportResponse;
import com.acme.payroll.v1.SearchPayRunsRequest;
import com.acme.payroll.v1.SearchPayRunsResponse;
import com.acme.payroll.v1.ShareDeductionReportRequest;
import com.acme.payroll.v1.ShareDeductionReportResponse;
import com.acme.payroll.v1.StreamDeductionMetricsRequest;
import com.acme.payroll.v1.StreamDeductionMetricsResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingBlockAction;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class DeductionReportingApplicationTest {
    @Test
    void scheduleDeductionReportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var listener = new DeductionReportingApplicationEvents(DeductionValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof DeductionReportingApplication.ScheduleDeductionReportStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new DeductionReportingApplication(
                ForecastSyncServiceGrpc.newStub(channel),
                ForecastSyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationWorkflowServiceGrpc.newBlockingStub(channel),
                ContractApprovalServiceGrpc.newBlockingStub(channel),
                ContractServiceGrpc.newStub(channel),
                events,
                UnderwritingReportingServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                DeductionCalculationServiceGrpc.newBlockingStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                PayRunQueryServiceGrpc.newBlockingStub(channel),
                PayRunServiceGrpc.newBlockingStub(channel),
                PayStubQueryServiceGrpc.newBlockingStub(channel),
                TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(ScheduleDeductionReportRequest.getDefaultInstance());
        var response = application.scheduleDeductionReport(request);
        var sent0 = channel.request("acme.payroll.v1.TaxWithholdingCalculationService/EstimateTaxWithholding");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "tax_withholding", "block", "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "tax_withholding", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "tax_withholding", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        var sent1 = channel.request("acme.payroll.v1.DeductionValidationService/NormalizeDeduction");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/EstimateTaxWithholding"), "tax_withholding", "discount", "amount"),
                ProbeSupport.value(sent1, "deduction", "discount", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/EstimateTaxWithholding"), "tax_withholding", "owner", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/EstimateTaxWithholding"), "tax_withholding", "owner", "user_id"),
                ProbeSupport.value(sent1, "deduction", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/EstimateTaxWithholding"), "tax_withholding", "audit", "created_by"),
                ProbeSupport.value(sent1, "deduction", "audit", "created_by"));
        var sent2 = channel.request("acme.ledger.v1.LedgerAccountNotificationService/ListLedgerAccountSubscriptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/EstimateTaxWithholding"), "tax_withholding", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/EstimateTaxWithholding"), "tax_withholding", "owner", "user_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/EstimateTaxWithholding"), "tax_withholding", "parameters"),
                ProbeSupport.value(sent2, "filter", "labels"));
        var sent3 = channel.request("acme.payroll.v1.PayRunService/SearchPayRuns");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/EstimateTaxWithholding"), "tax_withholding", "audit", "updated_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/EstimateTaxWithholding"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.DeductionValidationService/NormalizeDeduction"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunService/SearchPayRuns"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }

    @Test
    void streamDeductionMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var listener = new DeductionReportingApplicationEvents(DeductionValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof DeductionReportingApplication.ScheduleDeductionReportStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new DeductionReportingApplication(
                ForecastSyncServiceGrpc.newStub(channel),
                ForecastSyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationWorkflowServiceGrpc.newBlockingStub(channel),
                ContractApprovalServiceGrpc.newBlockingStub(channel),
                ContractServiceGrpc.newStub(channel),
                events,
                UnderwritingReportingServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                DeductionCalculationServiceGrpc.newBlockingStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                PayRunQueryServiceGrpc.newBlockingStub(channel),
                PayRunServiceGrpc.newBlockingStub(channel),
                PayStubQueryServiceGrpc.newBlockingStub(channel),
                TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(StreamDeductionMetricsRequest.getDefaultInstance());
        var response = application.streamDeductionMetrics(request);
        var sent0 = channel.request("acme.compliance.v1.AttestationWorkflowService/SignalAttestationWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(sent0, "details", "notify", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent0, "details", "notify", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationWorkflowService/SignalAttestationWorkflow"), "attestation", "audit", "created_by"),
                ProbeSupport.value(response, "deduction", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationWorkflowService/SignalAttestationWorkflow"), "attestation", "audit", "updated_by"),
                ProbeSupport.value(response, "deduction", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "deduction", "owner", "email"));
        var endpoint = new DeductionReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamDeductionMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamDeductionMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamDeductionMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new DeductionReportingApplicationEvents(DeductionValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<NormalizeDeductionResponse>();
        listener.on(new DeductionReportingApplication.ScheduleDeductionReportStage1Requested(ProbeSupport.sample(NormalizeDeductionRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
