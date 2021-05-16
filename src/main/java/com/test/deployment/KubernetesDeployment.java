package com.test.deployment;

import java.util.Collections;
import java.util.concurrent.Executors;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

public class KubernetesDeployment {
	public static void main(String[] args) {
		String flinkDistJar = "hdfs://data/flink/libs/flink-kubernetes_2.12-1.11.0.jar";
		Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration();
		flinkConfiguration.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
		flinkConfiguration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
		flinkConfiguration.set(PipelineOptions.JARS, Collections.singletonList(flinkDistJar));
		flinkConfiguration.set(KubernetesConfigOptions.CLUSTER_ID, "k8s-app1");
		flinkConfiguration.set(KubernetesConfigOptions.CONTAINER_IMAGE, "img_url");
		flinkConfiguration.set(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY, 
				KubernetesConfigOptions.ImagePullPolicy.Always);
		flinkConfiguration.set(KubernetesConfigOptions.JOB_MANAGER_CPU, 2.5);
		flinkConfiguration.set(KubernetesConfigOptions.TASK_MANAGER_CPU, 1.5);
		flinkConfiguration.set(JobManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.parse("4096M"));
		flinkConfiguration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("4096M"));
		flinkConfiguration.set(TaskManagerOptions.NUM_TASK_SLOTS, 1);
		
		KubernetesClient kubernetesClient = new DefaultKubernetesClient();
		FlinkKubeClient flinkKubeClient = new Fabric8FlinkKubeClient(flinkConfiguration, 
				kubernetesClient, () -> Executors.newFixedThreadPool(2));
		KubernetesClusterDescriptor kubernetesClusterDescriptor = new 
				KubernetesClusterDescriptor(flinkConfiguration, flinkKubeClient);
		
		ClusterSpecification clusterSpecification = new ClusterSpecification
				.ClusterSpecificationBuilder().createClusterSpecification();
		ApplicationConfiguration appConfig = new ApplicationConfiguration(args, null);
		
		ClusterClientProvider<String> clusterClientProvider = null;
		try {
			clusterClientProvider = kubernetesClusterDescriptor.deployApplicationCluster(
					clusterSpecification, appConfig);
		} catch (ClusterDeploymentException e) {
			e.printStackTrace();
		}
		kubernetesClusterDescriptor.close();
		ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient();
		String clusterId = clusterClient.getClusterId();
		System.out.println(clusterId);
	}
}
