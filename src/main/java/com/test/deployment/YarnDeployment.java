package com.test.deployment;

import java.util.Collections;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterInformationRetriever;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class YarnDeployment {
	public static void main(String[] args){

		System.setProperty("hadoop.home.dir", "E:\\software\\hadoop-3.3.0");
		//flink的本地配置目录，为了得到flink的配置
		String configurationDirectory = "E:\\software\\flink-1.11.2\\conf";
		//存放flink集群相关的jar包目录
		String flinkLibs = "hdfs://192.168.106.128:9000/data/flink/libs";
		//用户jar
		String userJarPath = "hdfs://192.168.106.128:9000/data/flink/user-lib/TopSpeedWindowing.jar";
		String flinkDistJar = "hdfs://192.168.106.128:9000/data/flink/libs/flink-yarn_2.11-1.11.0.jar";

		YarnClient yarnClient = YarnClient.createYarnClient();
		YarnConfiguration yarnConfiguration = new YarnConfiguration();
		yarnClient.init(yarnConfiguration);
		yarnClient.start();

		YarnClusterInformationRetriever clusterInformationRetriever = YarnClientYarnClusterInformationRetriever
				.create(yarnClient);

		//获取flink的配置
		Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration(
				configurationDirectory);
		flinkConfiguration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
		flinkConfiguration.set(
				PipelineOptions.JARS,
				Collections.singletonList(
						userJarPath));

		Path remoteLib = new Path(flinkLibs);
		flinkConfiguration.set(
				YarnConfigOptions.PROVIDED_LIB_DIRS,
				Collections.singletonList(remoteLib.toString()));

		flinkConfiguration.set(
				YarnConfigOptions.FLINK_DIST_JAR,
				flinkDistJar);
		//设置为application模式
		flinkConfiguration.set(
				DeploymentOptions.TARGET,
				YarnDeploymentTarget.APPLICATION.getName());
		//yarn application name
		flinkConfiguration.set(YarnConfigOptions.APPLICATION_NAME, "jobName");


		ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
				.createClusterSpecification();

//		设置用户jar的参数和主类
		ApplicationConfiguration appConfig = new ApplicationConfiguration(args, null);


		YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
				flinkConfiguration,
				yarnConfiguration,
				yarnClient,
				clusterInformationRetriever,
				true);
		ClusterClientProvider<ApplicationId> clusterClientProvider = null;
		try {
			clusterClientProvider = yarnClusterDescriptor.deployApplicationCluster(
					clusterSpecification,
					appConfig);
		} catch (ClusterDeploymentException e){
			e.printStackTrace();
		}
		yarnClusterDescriptor.close();
		ClusterClient<ApplicationId> clusterClient = clusterClientProvider.getClusterClient();
		ApplicationId applicationId = clusterClient.getClusterId();
		System.out.println(applicationId);
	}
}
