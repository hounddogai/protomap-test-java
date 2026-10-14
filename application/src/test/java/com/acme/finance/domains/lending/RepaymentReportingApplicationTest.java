package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.collections.v1.ListPendingPaymentPromisesRequest;
import com.acme.collections.v1.ListPendingPaymentPromisesResponse;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerCalculationRequest;
import com.acme.customers.v1.ExplainCustomerCalculationResponse;
import com.acme.customers.v1.ValidateCustomerRequest;
import com.acme.customers.v1.ValidateCustomerResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.GetUserRequest;
import com.acme.identity.v1.GetUserResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.NotifyPostingBatchWatchersRequest;
import com.acme.ledger.v1.NotifyPostingBatchWatchersResponse;
import com.acme.ledger.v1.ShareFiscalPeriodExportRequest;
import com.acme.ledger.v1.ShareFiscalPeriodExportResponse;
import com.acme.lending.v1.BrowseLoanApplicationsRequest;
import com.acme.lending.v1.BrowseLoanApplicationsResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CountRepaymentTotalsRequest;
import com.acme.lending.v1.CountRepaymentTotalsResponse;
import com.acme.lending.v1.DeleteRepaymentReportRequest;
import com.acme.lending.v1.DeleteRepaymentReportResponse;
import com.acme.lending.v1.FetchRepaymentReportRequest;
import com.acme.lending.v1.FetchRepaymentReportResponse;
import com.acme.lending.v1.GenerateRepaymentReportRequest;
import com.acme.lending.v1.GenerateRepaymentReportResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.lending.v1.QuoteRepaymentRequest;
import com.acme.lending.v1.QuoteRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ScheduleRepaymentReportRequest;
import com.acme.lending.v1.ScheduleRepaymentReportResponse;
import com.acme.lending.v1.ShareRepaymentReportRequest;
import com.acme.lending.v1.ShareRepaymentReportResponse;
import com.acme.lending.v1.StreamRepaymentMetricsRequest;
import com.acme.lending.v1.StreamRepaymentMetricsResponse;
import com.acme.lending.v1.SuggestLoanApplicationsRequest;
import com.acme.lending.v1.SuggestLoanApplicationsResponse;
import com.acme.venues.v1.PreviewVenueRequest;
import com.acme.venues.v1.PreviewVenueResponse;
import com.acme.venues.v1.SearchVenuesRequest;
import com.acme.venues.v1.SearchVenuesResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class RepaymentReportingApplicationTest {
    @Test
    void streamRepaymentMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var application = new RepaymentReportingApplication(
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PaymentPromiseApprovalServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                LoanApplicationQueryServiceGrpc.newBlockingStub(channel),
                LoanApplicationQueryServiceGrpc.newFutureStub(channel),
                RepaymentCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                VenueServiceGrpc.newBlockingStub(channel),
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamRepaymentMetricsRequest.getDefaultInstance());
        var response = application.streamRepaymentMetrics(request);
        var sent0 = channel.request("acme.identity.v1.UserService/GetUser");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "user_id"));
        var sent1 = channel.request("acme.venues.v1.VenueService/SearchVenues");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var sent2 = channel.request("acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/GetUser"), "user", "relationship_manager", "email"),
                ProbeSupport.value(sent2, "customer", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/GetUser"), "user", "audit", "updated_by"),
                ProbeSupport.value(response, "repayment", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/GetUser"), "user", "contact", "name"),
                ProbeSupport.value(response, "repayment", "participants", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueService/SearchVenues"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueService/SearchVenues"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation"), "customer", "contact", "phone"),
                ProbeSupport.value(response, "repayment", "participants", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "repayment", "location", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "repayment", "organizer", "email"));
        var endpoint = new RepaymentReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamRepaymentMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamRepaymentMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamRepaymentMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
