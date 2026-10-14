package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RevertAuditTrailRequest;
import com.acme.audit.v1.RevertAuditTrailResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.banking.v1.SubmitMandateRequest;
import com.acme.banking.v1.SubmitMandateResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.PaymentPromiseRef;
import com.acme.collections.v1.RetryCollectionCaseWorkflowRequest;
import com.acme.collections.v1.RetryCollectionCaseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.concessions.v1.BrowseConcessionStandsRequest;
import com.acme.concessions.v1.BrowseConcessionStandsResponse;
import com.acme.concessions.v1.ConcessionStandQueryServiceGrpc;
import com.acme.customers.v1.ComparePreferenceOptionsRequest;
import com.acme.customers.v1.ComparePreferenceOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationRequest;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationResponse;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.RecalculatePreferencesRequest;
import com.acme.customers.v1.RecalculatePreferencesResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentBankSettlement;
import com.acme.documents.v1.DocumentLine;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.fuel.v1.ExplainFuelCardCalculationRequest;
import com.acme.fuel.v1.ExplainFuelCardCalculationResponse;
import com.acme.fuel.v1.FuelCard;
import com.acme.fuel.v1.FuelCardCalculationServiceGrpc;
import com.acme.fuel.v1.FuelCardCardFunding;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.lending.v1.ActivateLoanRequest;
import com.acme.lending.v1.ActivateLoanResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.FindLoansRequest;
import com.acme.lending.v1.FindLoansResponse;
import com.acme.lending.v1.GetLoanLifecycleRequest;
import com.acme.lending.v1.GetLoanLifecycleResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanLimit;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.lending.v1.ReinstateLoanRequest;
import com.acme.lending.v1.ReinstateLoanResponse;
import com.acme.lending.v1.RenewLoanRequest;
import com.acme.lending.v1.RenewLoanResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.ScheduleLoanExpiryRequest;
import com.acme.lending.v1.ScheduleLoanExpiryResponse;
import com.acme.lending.v1.SuspendLoanRequest;
import com.acme.lending.v1.SuspendLoanResponse;
import com.acme.lending.v1.TerminateLoanRequest;
import com.acme.lending.v1.TerminateLoanResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SummarizeSearchIndexesRequest;
import com.acme.search.v1.SummarizeSearchIndexesResponse;
import com.acme.suppliers.v1.SupplierRef;
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

class LoanLifecycleApplicationTest {
    @Test
    void scheduleLoanExpiryPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var application = new LoanLifecycleApplication(
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                MandateApprovalServiceGrpc.newFutureStub(channel),
                ChannelRegistry.fixed(channel),
                ConcessionStandQueryServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                FuelCardCalculationServiceGrpc.newBlockingStub(channel),
                LoanQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel),
                SearchIndexQueryServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(ScheduleLoanExpiryRequest.getDefaultInstance());
        var response = application.scheduleLoanExpiry(request);
        var sent0 = channel.request("acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "contact", "name"),
                ProbeSupport.value(sent0, "details", "contact", "name"));
        var sent1 = channel.request("acme.fuel.v1.FuelCardCalculationService/ExplainFuelCardCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "card", "last4"),
                ProbeSupport.value(sent1, "fuel_card", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "card", "holder_name"),
                ProbeSupport.value(sent1, "fuel_card", "card", "holder_name"));
        var sent2 = channel.request("acme.customers.v1.PreferenceCalculationService/RecalculatePreferences");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.fuel.v1.FuelCardCalculationService/ExplainFuelCardCalculation"), "fuel_card", "holder", "display_name"),
                ProbeSupport.value(sent2, "preference", "owner", "display_name"));
        var sent3 = channel.request("acme.documents.v1.DocumentValidationService/PreviewDocument");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "limits", "amount"),
                ProbeSupport.value(sent3, "document", "lines", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.fuel.v1.FuelCardCalculationService/ExplainFuelCardCalculation"), "fuel_card", "contact", "phone"),
                ProbeSupport.value(sent3, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "bank", "iban"),
                ProbeSupport.value(sent3, "document", "bank", "iban"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "audit", "updated_by"),
                ProbeSupport.value(sent3, "document", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.fuel.v1.FuelCardCalculationService/ExplainFuelCardCalculation"), "fuel_card", "available"),
                ProbeSupport.value(response, "loan", "available"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.fuel.v1.FuelCardCalculationService/ExplainFuelCardCalculation"), "fuel_card", "audit", "updated_at"),
                ProbeSupport.value(response, "loan", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceCalculationService/RecalculatePreferences"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceCalculationService/RecalculatePreferences"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_id"),
                ProbeSupport.value(response, "loan", "loan_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "contact"),
                ProbeSupport.value(response, "loan", "contact"));
    }
}
