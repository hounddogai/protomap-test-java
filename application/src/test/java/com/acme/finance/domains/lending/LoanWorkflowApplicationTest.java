package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.broadcasts.v1.SuggestBroadcastsRequest;
import com.acme.broadcasts.v1.SuggestBroadcastsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.customers.v1.NormalizePreferenceRequest;
import com.acme.customers.v1.NormalizePreferenceResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceDiscountAction;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.documents.v1.CreateDocumentRequest;
import com.acme.documents.v1.CreateDocumentResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentCardSettlement;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CountPremiumTotalsRequest;
import com.acme.insurance.v1.CountPremiumTotalsResponse;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.ResolveUnderwritingConflictRequest;
import com.acme.insurance.v1.ResolveUnderwritingConflictResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesRequest;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesResponse;
import com.acme.invoicing.v1.InvoiceBatchServiceGrpc;
import com.acme.lending.v1.AdvanceLoanWorkflowRequest;
import com.acme.lending.v1.AdvanceLoanWorkflowResponse;
import com.acme.lending.v1.CancelLoanWorkflowRequest;
import com.acme.lending.v1.CancelLoanWorkflowResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.EscalateLoanRequest;
import com.acme.lending.v1.EscalateLoanResponse;
import com.acme.lending.v1.InspectLoanApplicationWorkflowRequest;
import com.acme.lending.v1.InspectLoanApplicationWorkflowResponse;
import com.acme.lending.v1.InspectLoanWorkflowRequest;
import com.acme.lending.v1.InspectLoanWorkflowResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanLifecycleServiceGrpc;
import com.acme.lending.v1.PauseLoanWorkflowRequest;
import com.acme.lending.v1.PauseLoanWorkflowResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RetryLoanWorkflowRequest;
import com.acme.lending.v1.RetryLoanWorkflowResponse;
import com.acme.lending.v1.ScheduleRepaymentExpiryRequest;
import com.acme.lending.v1.ScheduleRepaymentExpiryResponse;
import com.acme.lending.v1.SignalLoanWorkflowRequest;
import com.acme.lending.v1.SignalLoanWorkflowResponse;
import com.acme.lending.v1.StartLoanApplicationWorkflowRequest;
import com.acme.lending.v1.StartLoanApplicationWorkflowResponse;
import com.acme.lending.v1.StartLoanWorkflowRequest;
import com.acme.lending.v1.StartLoanWorkflowResponse;
import com.acme.lending.v1.TerminateLoanRequest;
import com.acme.lending.v1.TerminateLoanResponse;
import com.acme.sponsorships.v1.FindSponsorsRequest;
import com.acme.sponsorships.v1.FindSponsorsResponse;
import com.acme.sponsorships.v1.SponsorQueryServiceGrpc;
import com.acme.venues.v1.SuggestVenuesRequest;
import com.acme.venues.v1.SuggestVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.Optional;
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
import org.springframework.stereotype.Service;

class LoanWorkflowApplicationTest {
    @Test
    void pauseLoanWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var application = new LoanWorkflowApplication(
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                PremiumReportingServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newBlockingStub(channel),
                InvoiceBatchServiceGrpc.newStub(channel),
                LoanApplicationWorkflowServiceGrpc.newBlockingStub(channel),
                LoanApprovalServiceGrpc.newBlockingStub(channel),
                LoanLifecycleServiceGrpc.newBlockingStub(channel),
                new LoanWorkflowRequestMapperImpl(),
                repository,
                Runnable::run,
                SponsorQueryServiceGrpc.newBlockingStub(channel),
                VenueQueryServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(PauseLoanWorkflowRequest.getDefaultInstance());
        var response = application.pauseLoanWorkflow(request);
        var sent0 = channel.request("acme.insurance.v1.UnderwritingSyncService/ResolveUnderwritingConflict");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "audit", "updated_at"),
                ProbeSupport.value(sent0, "underwriting", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "audit", "created_by"),
                ProbeSupport.value(sent0, "underwriting", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "audit", "created_at"),
                ProbeSupport.value(sent0, "underwriting", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent0, "underwriting", "assignee", "email"));
        var sent1 = channel.request("acme.insurance.v1.PremiumReportingService/CountPremiumTotals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "labels"),
                ProbeSupport.value(sent1, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/ResolveUnderwritingConflict"), "underwriting", "sla", "due_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.lending.v1.LoanApprovalService/EscalateLoan");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "audit", "etag"),
                ProbeSupport.value(sent2, "loan", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "holder", "display_name"),
                ProbeSupport.value(sent2, "loan", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent2, "loan", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/ResolveUnderwritingConflict"), "underwriting", "assignee", "user_id"),
                ProbeSupport.value(sent2, "details", "approver", "user_id"));
        var sent3 = channel.request("acme.lending.v1.LoanLifecycleService/TerminateLoan");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "loan_id"),
                ProbeSupport.value(sent3, "loan_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/ResolveUnderwritingConflict"), "underwriting", "assignee", "email"),
                ProbeSupport.value(sent3, "details", "new_owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/ResolveUnderwritingConflict"), "underwriting", "requester", "preferred_channel"),
                ProbeSupport.value(sent3, "details", "notify_contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "contact", "phone"),
                ProbeSupport.value(sent3, "details", "notify_contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/ResolveUnderwritingConflict"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/ResolveUnderwritingConflict"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumReportingService/CountPremiumTotals"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApprovalService/EscalateLoan"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_id"),
                ProbeSupport.value(response, "loan", "loan_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "status"),
                ProbeSupport.value(response, "loan", "status"));
    }
}
