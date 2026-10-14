package com.acme.finance.domains.compliance;

import com.acme.collections.v1.CommentPaymentPromiseApprovalRequest;
import com.acme.collections.v1.CommentPaymentPromiseApprovalResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.collections.v1.RecallPaymentPromiseRequest;
import com.acme.collections.v1.RecallPaymentPromiseResponse;
import com.acme.collections.v1.StartPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.StartPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.StartWriteOffWorkflowRequest;
import com.acme.collections.v1.StartWriteOffWorkflowResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AdvanceControlWorkflowRequest;
import com.acme.compliance.v1.AdvanceControlWorkflowResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.AttestationLine;
import com.acme.compliance.v1.AttestationLineTax;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.CancelControlWorkflowRequest;
import com.acme.compliance.v1.CancelControlWorkflowResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlCondition;
import com.acme.compliance.v1.ControlLifecycleServiceGrpc;
import com.acme.compliance.v1.DelegateAttestationApprovalRequest;
import com.acme.compliance.v1.DelegateAttestationApprovalResponse;
import com.acme.compliance.v1.GetControlApprovalStatusRequest;
import com.acme.compliance.v1.GetControlApprovalStatusResponse;
import com.acme.compliance.v1.GetControlRatesRequest;
import com.acme.compliance.v1.GetControlRatesResponse;
import com.acme.compliance.v1.RejectControlRequest;
import com.acme.compliance.v1.RejectControlResponse;
import com.acme.compliance.v1.ResumeControlWorkflowRequest;
import com.acme.compliance.v1.ResumeControlWorkflowResponse;
import com.acme.compliance.v1.RetryControlWorkflowRequest;
import com.acme.compliance.v1.RetryControlWorkflowResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.ScheduleControlExpiryRequest;
import com.acme.compliance.v1.ScheduleControlExpiryResponse;
import com.acme.compliance.v1.ShareAttestationReportRequest;
import com.acme.compliance.v1.ShareAttestationReportResponse;
import com.acme.compliance.v1.SignalControlWorkflowRequest;
import com.acme.compliance.v1.SignalControlWorkflowResponse;
import com.acme.compliance.v1.StartControlWorkflowRequest;
import com.acme.compliance.v1.StartControlWorkflowResponse;
import com.acme.configuration.v1.BrowseConfigSetsRequest;
import com.acme.configuration.v1.BrowseConfigSetsResponse;
import com.acme.configuration.v1.ConfigSetQueryServiceGrpc;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.ExportDocumentsRequest;
import com.acme.documents.v1.ExportDocumentsResponse;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagRouteAction;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.LintFeatureFlagsRequest;
import com.acme.featureflags.v1.LintFeatureFlagsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanLimit;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.PauseLoanWorkflowRequest;
import com.acme.lending.v1.PauseLoanWorkflowResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class ControlWorkflowApplicationTest {
    @Test
    void startControlWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new ControlWorkflowApplication(
                ChannelRegistry.fixed(channel),
                PaymentPromiseApprovalServiceGrpc.newBlockingStub(channel),
                PaymentPromiseWorkflowServiceGrpc.newBlockingStub(channel),
                WriteOffWorkflowServiceGrpc.newBlockingStub(channel),
                AttestationReportingServiceGrpc.newBlockingStub(channel),
                ControlApprovalServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                ControlLifecycleServiceGrpc.newBlockingStub(channel),
                ConfigSetQueryServiceGrpc.newBlockingStub(channel),
                DocumentQueryServiceGrpc.newBlockingStub(channel),
                FeatureFlagServiceGrpc.newBlockingStub(channel),
                LoanWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(StartControlWorkflowRequest.getDefaultInstance());
        var response = application.startControlWorkflow(request);
        var sent0 = channel.request("acme.compliance.v1.ControlCalculationService/GetControlRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "control_id"),
                ProbeSupport.value(sent0, "control_id"));
        var sent1 = channel.request("acme.configuration.v1.ConfigSetQueryService/BrowseConfigSets");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/GetControlRates"), "control", "owner", "user_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        var sent2 = channel.request("acme.compliance.v1.ControlLifecycleService/ScheduleControlExpiry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "conditions", "operator"),
                ProbeSupport.value(sent2, "control", "conditions", "operator"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "conditions", "attribute"),
                ProbeSupport.value(sent2, "control", "conditions", "attribute"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "conditions", "value"),
                ProbeSupport.value(sent2, "control", "conditions", "value"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "conditions", "negate"),
                ProbeSupport.value(sent2, "control", "conditions", "negate"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/GetControlRates"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.configuration.v1.ConfigSetQueryService/BrowseConfigSets"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control_id"),
                ProbeSupport.value(response, "control", "control_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "version"),
                ProbeSupport.value(response, "control", "version"));
    }
}
