package io.temporal.samples.hello;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.workflow.*;

import java.util.ArrayList;
import java.util.List;

public class ParentChildWorkflow {

    // Define the task queue name
    static final String TASK_QUEUE = "HelloActivityTaskQueue";


   @WorkflowInterface
    public interface ParentWorkflow {

        @WorkflowMethod
        void execute();

    }

    @WorkflowInterface
    public interface ChildWorkflow {

        @WorkflowMethod
        void execute();
    }





    public static class ParentWorkflowImpl implements ParentWorkflow {

        @Override
        public void execute() {

            List<Promise<WorkflowExecution>> executionResults = new ArrayList<>();
            List<Promise<Void>> results = new ArrayList<>();

            for(int i=0 ;i< 150; i++) {
                ChildWorkflow childWf =
                        Workflow.newChildWorkflowStub(ChildWorkflow.class, ChildWorkflowOptions.newBuilder()
                                .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
                                .setWorkflowId("workflow-"+i)
                                .build());

                results.add(Async.procedure(childWf::execute));
                executionResults.add(Workflow.getWorkflowExecution(childWf));

            }

            //wait for all childworkflows to get spawned
            Promise.allOf(executionResults).get();
            //wait for all childworkflows to complete
            Promise.allOf(results).get();


        }

    }

    // Define the workflow implementation which implements our getGreeting workflow method.
    public static class ChildWorkflowImpl implements ChildWorkflow {


        @Override
        public void execute() {
            //do nothing
        }

    }



    public static void main(String[] args) {

        WorkflowServiceStubs service = WorkflowServiceStubs.newInstance();

        WorkflowClient client = WorkflowClient.newInstance(service);

        WorkerFactory factory =
                WorkerFactory.newInstance(
                        client, WorkerFactoryOptions.newBuilder().setEnableLoggingInReplay(false).build());

        Worker worker = factory.newWorker(TASK_QUEUE);

        worker.registerWorkflowImplementationTypes(ParentWorkflowImpl.class, ChildWorkflowImpl.class);

       factory.start();


        ParentWorkflow workflow =
                client.newWorkflowStub(
                        ParentWorkflow.class,
                        WorkflowOptions.newBuilder()
                                .setTaskQueue(TASK_QUEUE)
                                .build());

       workflow.execute();


    }
}
