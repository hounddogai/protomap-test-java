package com.acme.finance.jobs.receivablesreconciliation;

import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.BillingAccountQueryServiceGrpc;
import com.acme.billing.v1.ListPendingBillingAccountsRequest;
import com.acme.billing.v1.LookupBillingAccountRequest;
import com.acme.common.v1.Actor;
import com.acme.common.v1.RequestContext;
import com.acme.finance.platform.CallIdentity;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.server.FinanceApplication;
import io.grpc.Context;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Runs a bounded reconciliation pass. Scheduling and tenant selection belong to the job runner.
 */
@Configuration(proxyBeanMethods = false)
@Profile("job-receivablesreconciliation")
public class ReceivablesReconciliation {
    public static void main(String[] args) {
        try (var application = new SpringApplicationBuilder(FinanceApplication.class).profiles("job-receivablesreconciliation")
                .properties("grpc.server.port=-1").run(args)) {
            application.getBean(ChannelRegistry.class);
        }
    }

    @Bean
    ApplicationRunner runReceivablesReconciliation(ChannelRegistry channels, org.springframework.core.env.Environment environment) {
        return arguments -> {
            String tenant = environment.getRequiredProperty("acme.job.tenant");
            var context = RequestContext.newBuilder().setTenantId(tenant)
                    .setActor(Actor.newBuilder().setServiceAccount(true)).build();
            // The channels' forwarding interceptor gives the job's calls its own credential and tenant.
            Context job = Context.current().withValue(CallIdentity.TENANT, tenant).withValue(CallIdentity.AUTHORIZATION,
                    "Bearer " + environment.getRequiredProperty("acme.job.token"));
            Context previous = job.attach();
            try {
                var source = BillingAccountApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.BillingAccountApprovalService"));
                var target = BillingAccountQueryServiceGrpc.newBlockingStub(channels.lookup("acme.billing.v1.BillingAccountQueryService"));
                var page = source.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingBillingAccounts(
                        ListPendingBillingAccountsRequest.newBuilder().setContext(context).build());
                for (var record : page.getBillingAccountsList()) {
                    var request = LookupBillingAccountRequest.newBuilder()
        .setBillingAccountId(record.getBillingAccountId())
        .setContext(context)
        .build();
                    target.withDeadlineAfter(8, TimeUnit.SECONDS).lookupBillingAccount(request);
                }
            } finally {
                job.detach(previous);
            }
        };
    }
}
