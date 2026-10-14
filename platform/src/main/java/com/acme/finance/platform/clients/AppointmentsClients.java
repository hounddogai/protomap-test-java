package com.acme.finance.platform.clients;

import com.acme.appointments.v1.AppointmentQueryServiceGrpc;
import com.acme.appointments.v1.AppointmentServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the appointments contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class AppointmentsClients {
    @Bean
    public AppointmentQueryServiceGrpc.AppointmentQueryServiceBlockingStub appointmentsAppointmentQueryServiceBlocking(ChannelRegistry channels) {
        return AppointmentQueryServiceGrpc.newBlockingStub(channels.lookup("acme.appointments.v1.AppointmentQueryService"));
    }

    @Bean
    public AppointmentServiceGrpc.AppointmentServiceBlockingStub appointmentsAppointmentServiceBlocking(ChannelRegistry channels) {
        return AppointmentServiceGrpc.newBlockingStub(channels.lookup("acme.appointments.v1.AppointmentService"));
    }
}
