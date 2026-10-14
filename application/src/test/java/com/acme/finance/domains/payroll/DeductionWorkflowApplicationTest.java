package com.acme.finance.domains.payroll;

import com.acme.benefits.v1.BenefitPlanServiceGrpc;
import com.acme.benefits.v1.WatchBenefitPlansRequest;
import com.acme.benefits.v1.WatchBenefitPlansResponse;
import com.acme.certificates.v1.BatchGetCertificatesRequest;
import com.acme.certificates.v1.BatchGetCertificatesResponse;
import com.acme.certificates.v1.CertificateServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchLine;
import com.acme.ledger.v1.PostingBatchLineTax;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.lineage.v1.CountLineageNodesRequest;
import com.acme.lineage.v1.CountLineageNodesResponse;
import com.acme.lineage.v1.LineageNodeQueryServiceGrpc;
import com.acme.payroll.v1.AdvanceDeductionWorkflowRequest;
import com.acme.payroll.v1.AdvanceDeductionWorkflowResponse;
import com.acme.payroll.v1.ApproveDeductionRequest;
import com.acme.payroll.v1.ApproveDeductionResponse;
import com.acme.payroll.v1.CancelDeductionWorkflowRequest;
import com.acme.payroll.v1.CancelDeductionWorkflowResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionApprovalServiceGrpc;
import com.acme.payroll.v1.DeductionCondition;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.EscalateTaxWithholdingRequest;
import com.acme.payroll.v1.EscalateTaxWithholdingResponse;
import com.acme.payroll.v1.InspectDeductionWorkflowRequest;
import com.acme.payroll.v1.InspectDeductionWorkflowResponse;
import com.acme.payroll.v1.ListDeductionWorkflowsRequest;
import com.acme.payroll.v1.ListDeductionWorkflowsResponse;
import com.acme.payroll.v1.PauseDeductionWorkflowRequest;
import com.acme.payroll.v1.PauseDeductionWorkflowResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PayRunWorkflowServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PreviewPayRunRequest;
import com.acme.payroll.v1.PreviewPayRunResponse;
import com.acme.payroll.v1.RecallTaxWithholdingRequest;
import com.acme.payroll.v1.RecallTaxWithholdingResponse;
import com.acme.payroll.v1.ResumeDeductionWorkflowRequest;
import com.acme.payroll.v1.ResumeDeductionWorkflowResponse;
import com.acme.payroll.v1.RetryDeductionWorkflowRequest;
import com.acme.payroll.v1.RetryDeductionWorkflowResponse;
import com.acme.payroll.v1.RetryPayRunWorkflowRequest;
import com.acme.payroll.v1.RetryPayRunWorkflowResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportResponse;
import com.acme.payroll.v1.ShareTaxWithholdingReportRequest;
import com.acme.payroll.v1.ShareTaxWithholdingReportResponse;
import com.acme.payroll.v1.SignalDeductionWorkflowRequest;
import com.acme.payroll.v1.SignalDeductionWorkflowResponse;
import com.acme.payroll.v1.StartDeductionWorkflowRequest;
import com.acme.payroll.v1.StartDeductionWorkflowResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingApprovalServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingRouteAction;
import com.acme.search.v1.BatchGetSearchIndexesRequest;
import com.acme.search.v1.BatchGetSearchIndexesResponse;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class DeductionWorkflowApplicationTest {
    @Test
    void cancelDeductionWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var listener = new DeductionWorkflowApplicationEvents(PayRunValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof DeductionWorkflowApplication.CancelDeductionWorkflowStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new DeductionWorkflowApplication(
                BenefitPlanServiceGrpc.newBlockingStub(channel),
                CertificateServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                LineageNodeQueryServiceGrpc.newBlockingStub(channel),
                DeductionApprovalServiceGrpc.newBlockingStub(channel),
                PayRunWorkflowServiceGrpc.newBlockingStub(channel),
                TaxWithholdingApprovalServiceGrpc.newBlockingStub(channel),
                TaxWithholdingReportingServiceGrpc.newBlockingStub(channel),
                repository,
                SearchIndexServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CancelDeductionWorkflowRequest.getDefaultInstance());
        var response = application.cancelDeductionWorkflow(request);
        var sent0 = channel.request("acme.payroll.v1.TaxWithholdingReportingService/ScheduleTaxWithholdingReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        var sent1 = channel.request("acme.benefits.v1.BenefitPlanService/WatchBenefitPlans");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent1, "subscriber", "preferred_channel"));
        var sent2 = channel.request("acme.payroll.v1.PayRunValidationService/PreviewPayRun");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "discount", "amount"),
                ProbeSupport.value(sent2, "pay_run", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "audit", "etag"),
                ProbeSupport.value(sent2, "pay_run", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "version"),
                ProbeSupport.value(sent2, "pay_run", "counterparty", "version"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "description"),
                ProbeSupport.value(sent2, "pay_run", "lines", "description"));
        var sent3 = channel.request("acme.certificates.v1.CertificateService/BatchGetCertificates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingReportingService/ScheduleTaxWithholdingReport"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunValidationService/PreviewPayRun"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunValidationService/PreviewPayRun"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction_id"),
                ProbeSupport.value(response, "deduction", "deduction_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "version"),
                ProbeSupport.value(response, "deduction", "version"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new DeductionWorkflowApplicationEvents(PayRunValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<PreviewPayRunResponse>();
        listener.on(new DeductionWorkflowApplication.CancelDeductionWorkflowStage2Requested(ProbeSupport.sample(PreviewPayRunRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
