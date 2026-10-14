package com.acme.finance.domains.payroll;

import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.ListAccessPoliciesRequest;
import com.acme.authorization.v1.ListAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.BrowseForumsRequest;
import com.acme.community.v1.BrowseForumsResponse;
import com.acme.community.v1.ForumQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.SuggestJournalEntriesRequest;
import com.acme.ledger.v1.SuggestJournalEntriesResponse;
import com.acme.licensing.v1.ListSoftwareLicensesRequest;
import com.acme.licensing.v1.ListSoftwareLicensesResponse;
import com.acme.licensing.v1.SoftwareLicenseServiceGrpc;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.payroll.v1.BatchCreatePayRunsRequest;
import com.acme.payroll.v1.BatchCreatePayRunsResponse;
import com.acme.payroll.v1.BatchGetPayRunsRequest;
import com.acme.payroll.v1.BatchGetPayRunsResponse;
import com.acme.payroll.v1.BrowsePayRunsRequest;
import com.acme.payroll.v1.BrowsePayRunsResponse;
import com.acme.payroll.v1.CalculateTaxWithholdingRequest;
import com.acme.payroll.v1.CalculateTaxWithholdingResponse;
import com.acme.payroll.v1.CheckTaxWithholdingEligibilityRequest;
import com.acme.payroll.v1.CheckTaxWithholdingEligibilityResponse;
import com.acme.payroll.v1.ClonePayRunRequest;
import com.acme.payroll.v1.ClonePayRunResponse;
import com.acme.payroll.v1.CreatePayRunRequest;
import com.acme.payroll.v1.CreatePayRunResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.ExplainPayStubDecisionRequest;
import com.acme.payroll.v1.ExplainPayStubDecisionResponse;
import com.acme.payroll.v1.GetPayRunRatesRequest;
import com.acme.payroll.v1.GetPayRunRatesResponse;
import com.acme.payroll.v1.GetPayRunRequest;
import com.acme.payroll.v1.GetPayRunResponse;
import com.acme.payroll.v1.LintTaxWithholdingsRequest;
import com.acme.payroll.v1.LintTaxWithholdingsResponse;
import com.acme.payroll.v1.ListPayRunsRequest;
import com.acme.payroll.v1.ListPayRunsResponse;
import com.acme.payroll.v1.NormalizeDeductionRequest;
import com.acme.payroll.v1.NormalizeDeductionResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubBankSettlement;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.ScoreDeductionRequest;
import com.acme.payroll.v1.ScoreDeductionResponse;
import com.acme.payroll.v1.SearchPayRunsRequest;
import com.acme.payroll.v1.SearchPayRunsResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingDiscountAction;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.payroll.v1.UpdatePayRunRequest;
import com.acme.payroll.v1.UpdatePayRunResponse;
import com.acme.procurement.v1.PreviewRfqRequest;
import com.acme.procurement.v1.PreviewRfqResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqComment;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class PayRunApplicationTest {
    @Test
    void updatePayRunPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var listener = new PayRunApplicationEvents(DeductionValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PayRunApplication.BatchCreatePayRunsStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PayRunApplication(
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ForumQueryServiceGrpc.newBlockingStub(channel),
                events,
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                SoftwareLicenseServiceGrpc.newBlockingStub(channel),
                new PayRunRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                DeductionValidationServiceGrpc.newBlockingStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                PayRunQueryServiceGrpc.newBlockingStub(channel),
                PayStubValidationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(UpdatePayRunRequest.getDefaultInstance());
        var response = application.updatePayRun(request);
        var sent0 = channel.request("acme.ledger.v1.JournalEntryQueryService/SuggestJournalEntries");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "due_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "metadata"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "pay_run_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.payroll.v1.PayStubValidationService/ExplainPayStubDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "bank", "bic"),
                ProbeSupport.value(sent1, "pay_stub", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "audit", "updated_at"),
                ProbeSupport.value(sent1, "pay_stub", "audit", "updated_at"));
        var sent2 = channel.request("acme.authorization.v1.AccessPolicyService/ListAccessPolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubValidationService/ExplainPayStubDecision"), "pay_stub", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        var sent3 = channel.request("acme.payroll.v1.PayRunCalculationService/GetPayRunRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "pay_run_id"),
                ProbeSupport.value(sent3, "pay_run_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryQueryService/SuggestJournalEntries"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryQueryService/SuggestJournalEntries"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubValidationService/ExplainPayStubDecision"), "pay_stub", "issued_at"),
                ProbeSupport.value(response, "pay_run", "issued_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "due_at"),
                ProbeSupport.value(response, "pay_run", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "notes"),
                ProbeSupport.value(response, "pay_run", "notes"));
    }

    @Test
    void batchCreatePayRunsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var listener = new PayRunApplicationEvents(DeductionValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PayRunApplication.BatchCreatePayRunsStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PayRunApplication(
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ForumQueryServiceGrpc.newBlockingStub(channel),
                events,
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                SoftwareLicenseServiceGrpc.newBlockingStub(channel),
                new PayRunRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                DeductionValidationServiceGrpc.newBlockingStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                PayRunQueryServiceGrpc.newBlockingStub(channel),
                PayStubValidationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(BatchCreatePayRunsRequest.getDefaultInstance());
        var response = application.batchCreatePayRuns(request);
        var sent0 = channel.request("acme.metrics.v1.MetricSeriesService/ListMetricSerieses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "issued_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "counterparty", "deduction_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.payroll.v1.DeductionValidationService/ScoreDeduction");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "created_at"),
                ProbeSupport.value(sent1, "deduction", "audit", "created_at"));
        var sent2 = channel.request("acme.licensing.v1.SoftwareLicenseService/ListSoftwareLicenses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesService/ListMetricSerieses"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.DeductionValidationService/ScoreDeduction"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.licensing.v1.SoftwareLicenseService/ListSoftwareLicenses"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        var endpoint = new PayRunEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreatePayRunsResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreatePayRunsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreatePayRuns(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new PayRunApplicationEvents(DeductionValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ScoreDeductionResponse>();
        listener.on(new PayRunApplication.BatchCreatePayRunsStage1Requested(ProbeSupport.sample(ScoreDeductionRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
