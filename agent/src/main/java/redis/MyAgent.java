package redis;

import io.activej.service.adapter.ServiceAdapters;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.instrument.Instrumentation;

@Deprecated
// performance bad
public class MyAgent {
    public static void premain(String agentArgs, Instrumentation inst) {
        System.out.println("My agent is running with args: " + agentArgs);

        new AgentBuilder.Default()
                .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                .with(AgentBuilder.TypeStrategy.Default.REDEFINE)
                .type(ElementMatchers.is(ServiceAdapters.class))
                .transform((builder, typeDescription, classLoader, module, protectionDomain) -> {
                            System.out.println("My agent is transforming ServiceAdapters class.");

                            var rawMethod = builder.method(ElementMatchers.isStatic()
                                    .and(ElementMatchers.named("forEventloop"))
                                    .and(ElementMatchers.takesNoArguments()));

                            return rawMethod.intercept(MethodDelegation.to(MyInterceptor.class));
                        }

                )
                .installOn(inst);
    }
}
