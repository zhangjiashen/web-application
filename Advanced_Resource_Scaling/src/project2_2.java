import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import java.net.HttpURLConnection;
import java.net.URL;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.DeleteAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.DeleteLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.InstanceMonitoring;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyRequest;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyResult;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.ComparisonOperator;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.Statistic;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DeleteSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.Placement;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.ConfigureHealthCheckRequest;
import com.amazonaws.services.elasticloadbalancing.model.ConfigureHealthCheckResult;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.CrossZoneLoadBalancing;
import com.amazonaws.services.elasticloadbalancing.model.DeleteLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancing.model.DescribeLoadBalancersResult;
import com.amazonaws.services.elasticloadbalancing.model.HealthCheck;
import com.amazonaws.services.elasticloadbalancing.model.Listener;
import com.amazonaws.services.elasticloadbalancing.model.LoadBalancerAttributes;
import com.amazonaws.services.elasticloadbalancing.model.ModifyLoadBalancerAttributesRequest;
import com.amazonaws.services.elasticloadbalancing.model.ModifyLoadBalancerAttributesResult;
import com.amazonaws.services.elasticloadbalancing.model.RegisterInstancesWithLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.RegisterInstancesWithLoadBalancerResult;

public class project2_2 {
	
	
	/*  References for project 2.1:
	 *  http://stackoverflow.com/questions/8828964/getting-state-of-ec2-instance-java-api
	 *  http://stackoverflow.com/questions/12065059/from-the-aws-java-api-how-can-i-tell-when-my-ebs-snapshot-has-been-created
	 *  http://stackoverflow.com/questions/9241584/get-public-dns-of-amazon-ec2-instance-from-java-api
	 *  http://stackoverflow.com/questions/1485708/how-do-i-do-a-http-get-in-java
	 *  http://stackoverflow.com/questions/4205980/java-sending-http-parameters-via-post-method-easily
	 *  http://stackoverflow.com/questions/3324717/sending-http-post-request-in-java
	 */
	//Data and parameters
	private static final long SLEEP_CYCLE = 60000;
	private static String securityGroup = "All";
	private static String keyName = "project0demo";
	private static String LGinstanceType = "m3.medium";
	private static String DCinstanceType = "m3.large";
	private static String lgAMIid = "ami-312b5154";
	private static String dcAMIid = "ami-3b2b515e";
	private static String ELBname = "ELB1";
	private static List<String> instances = new ArrayList<String>();
	private static BasicAWSCredentials bawsc;
	private static AmazonEC2Client ec2;
	private static AmazonElasticLoadBalancingClient elb;
	private static AmazonAutoScalingClient autoScaling;
	private static AmazonCloudWatchClient cloudWatch;
	private static String avZone = "us-east-1b";
	private static String launchConfigName = "launchConfigProject2";
	private static String ASGname = "AutoScaling1";
	//Methods
	public static void main(String[] args) throws Exception{
		initialize();

		//Create Instance Request
		RunInstancesRequest runLoadGeneratorRequest = new RunInstancesRequest();
		RunInstancesRequest runDataCenterRequest = new RunInstancesRequest();
		//Configure Instance Request
		Placement placement = new Placement(avZone);
		
		runLoadGeneratorRequest.withImageId(lgAMIid)
		.withInstanceType(LGinstanceType)
		.withMinCount(1)
		.withMaxCount(1)
		.withKeyName(keyName)
		.withSecurityGroups(securityGroup)
		.withPlacement(placement);
		/*
		runDataCenterRequest.withImageId(dcAMIid)
		.withInstanceType(DCinstanceType)
		.withMinCount(1)
		.withMaxCount(1)
		.withKeyName(keyName)
		.withSecurityGroups(securityGroup);
		*/
		/*
		 * Launch a load generator
		 */
		Tag tag = new Tag("Project","2.2");
		String lgID = launchLG(runLoadGeneratorRequest, tag);
		String lgDNS = getLGdns(lgID);
		submitPassword(lgDNS, args[0]);

		/*
		 * Launch a data center and wait for it being ready
		
		String dcID = launchDC(runDataCenterRequest, tag);
		String dcDNS = getDCdns(dcID);
		 */
		/*
		 * Create Security Group
		 */
		String ELBSecurityGroupName = "ELBSecurityGroup";
		String ELBSecurityGroupID = newSecurityGroup(ELBSecurityGroupName);
		ArrayList<String> securityGroups = new ArrayList<String>();
		securityGroups.add(ELBSecurityGroupID);
		/*
		 * launch an ELB
		 * http://stackoverflow.com/questions/10374704/how-can-i-create-a-load-balancer-in-aws-using-the-aws-java-sdk
		 * http://marksdevserver.com/2013/03/11/setting-autoscaling-java/
		 */
		
		
		  //create load balancer
		com.amazonaws.services.elasticloadbalancing.model.Tag ELBtag = 
				new com.amazonaws.services.elasticloadbalancing.model.Tag().withKey("Project").withValue("2.2");
		String elbDNS = newELB(ELBtag, securityGroups);

		
		
		
        //health check
        int interval = 60;
        int timeout = 5;
        int unhealthyThreshold = 2;
        int healthyThreshold = 3;
        String healthCheckTarget = "HTTP:80/heartbeat?lg=" + lgDNS;
		HealthCheck healthCheck = new HealthCheck(healthCheckTarget, interval, timeout, unhealthyThreshold, healthyThreshold);
		ConfigureHealthCheckRequest configureHealthCheckRequest = new ConfigureHealthCheckRequest(ELBname, healthCheck);
		ConfigureHealthCheckResult configureHealthCheckResult = elb.configureHealthCheck(configureHealthCheckRequest);
		
		//register a instance to the balancer
		//addInstanceToELB(dcID);
		
		/*
		 * Create Auto Scaling Group
		 */
		
		com.amazonaws.services.autoscaling.model.Tag ASGtag = 
				new com.amazonaws.services.autoscaling.model.Tag().withKey("Project").withValue("2.2");
		ArrayList<com.amazonaws.services.autoscaling.model.Tag> ASGtags =
				new ArrayList<com.amazonaws.services.autoscaling.model.Tag>();
		ASGtags.add(ASGtag);
		myAutoScalingGroup(securityGroups, ASGtags);
		System.out.println("AutoScaling Group created!");
		Thread.sleep(5*SLEEP_CYCLE);
		/*
		 * warm-up
		 */
		warmup(lgDNS, elbDNS);
		Thread.sleep(6*SLEEP_CYCLE);
		warmup(lgDNS, elbDNS);
		Thread.sleep(6*SLEEP_CYCLE);
		warmup(lgDNS, elbDNS);
		Thread.sleep(6*SLEEP_CYCLE);
		warmup(lgDNS, elbDNS);
		Thread.sleep(6*SLEEP_CYCLE);
		warmup(lgDNS, elbDNS);
		Thread.sleep(6*SLEEP_CYCLE);
		
		updateASG(); //update Min and Max
		Thread.sleep(5000);
		
		juniorTest(lgDNS, elbDNS);
		
		
		Thread.sleep(55*SLEEP_CYCLE);
		terminate();			
		Thread.sleep(5*SLEEP_CYCLE);
		delASG();
		Thread.sleep(5*SLEEP_CYCLE);
        deleteLaunchConfiguration();
        Thread.sleep(5*SLEEP_CYCLE);
        deleteSG(ELBSecurityGroupName);


		
	}
	
	
	private static void juniorTest(String lgDNS, String elbDNS) throws IOException, InterruptedException {
		int response;
		String tempLine;
		StringBuilder result = new StringBuilder();
		URL submitDNSURL = new URL("http://" + lgDNS + "/junior?dns=" + elbDNS);
		HttpURLConnection submitDNS = (HttpURLConnection) submitDNSURL.openConnection();
		submitDNS.setRequestMethod("GET");
		while ((response = submitDNS.getResponseCode()) != 200){
			System.out.println("load generator response" + response);
			Thread.sleep(1000);
		}
		System.out.println("Junior test begined successfully!");
		
		InputStreamReader inputStreamReader = new InputStreamReader(submitDNS.getInputStream());
		BufferedReader bufferedReader2 = new BufferedReader(inputStreamReader);
		result = new StringBuilder();
		while ((tempLine = bufferedReader2.readLine()) != null){
			result.append(tempLine);
		}
		System.out.println(result);
		bufferedReader2.close();
		int index;
		int endindex;
		//float totalrps = 0;
		//int rpsindex;
		String testlog;
		if ((index = result.toString().lastIndexOf("/log?name=test.")) != -1){
			endindex = result.toString().indexOf(".log", index);
			testlog = result.toString().substring(index+15, endindex);
			System.out.println(testlog);
			
			URL logURL = new URL("http://" + lgDNS + "/log?name=test." + testlog + ".log");
			HttpURLConnection logConn = (HttpURLConnection) logURL.openConnection();
			logConn.setRequestMethod("GET");
			while ((response = logConn.getResponseCode()) != 200){
				System.out.println("log response" + response);
				Thread.sleep(1000);
			}
			inputStreamReader = new InputStreamReader(logConn.getInputStream());
			BufferedReader bufferedReader3 = new BufferedReader(inputStreamReader);
			//result = new StringBuilder();
			while ((tempLine = bufferedReader3.readLine()) != null){
				System.out.println(tempLine);

			}
			bufferedReader3.close();
			//System.out.println(totalrps);
		
		}
	}


	private static void warmup(String lgDNS, String elbDNS) throws IOException, InterruptedException {
		int response;
		URL submitDNSURL = new URL("http://" + lgDNS + "/warmup?dns=" + elbDNS);
		HttpURLConnection submitDNS = (HttpURLConnection) submitDNSURL.openConnection();
		submitDNS.setRequestMethod("GET");
		while ((response = submitDNS.getResponseCode()) != 200){
			System.out.println("load generator warm-up response" + response);
			Thread.sleep(1000);
		}
		System.out.println("Warm-up begined successfully!");
		
	}


	private static void deleteLaunchConfiguration() {
		DeleteLaunchConfigurationRequest dellcRequest = new DeleteLaunchConfigurationRequest();
        dellcRequest.setLaunchConfigurationName(launchConfigName);
        autoScaling.deleteLaunchConfiguration(dellcRequest);
        System.out.println("Launch Configuration Deleted!");
	}


	private static String newELB(com.amazonaws.services.elasticloadbalancing.model.Tag elbTag, ArrayList<String> securityGroups) throws InterruptedException {
        CreateLoadBalancerRequest lbRequest = new CreateLoadBalancerRequest();
        lbRequest.setLoadBalancerName(ELBname);
        List<Listener> listeners = new ArrayList<Listener>(1);
        listeners.add(new Listener("HTTP", 80, 80));
        lbRequest.withAvailabilityZones(avZone);
        lbRequest.setListeners(listeners);
        lbRequest.setSecurityGroups(securityGroups);
        lbRequest.withTags(elbTag);
        CreateLoadBalancerResult lbResult=elb.createLoadBalancer(lbRequest);
        System.out.println("created load balancer loader. Sleep for 5s");
		Thread.sleep(5000);
		ArrayList<String> ELBs = new ArrayList<String>();
		ELBs.add(ELBname);
		String elbDNS = "";
		while (elbDNS.isEmpty()){
			DescribeLoadBalancersRequest describeLoadBalancersRequest = new DescribeLoadBalancersRequest(ELBs);
			DescribeLoadBalancersResult describeLBresult = elb.describeLoadBalancers(describeLoadBalancersRequest);
			elbDNS = describeLBresult.getLoadBalancerDescriptions().get(0).getDNSName();
			System.out.println("ELB DNS: " + elbDNS);
		}
		// disable cross-zone load balancing
		ModifyLoadBalancerAttributesRequest modifyLoadBalancerAttributesRequest = new ModifyLoadBalancerAttributesRequest();
		modifyLoadBalancerAttributesRequest.setLoadBalancerName(ELBname);
		CrossZoneLoadBalancing crossZoneLoadBalancing = new CrossZoneLoadBalancing();
		crossZoneLoadBalancing.setEnabled(Boolean.FALSE);
		LoadBalancerAttributes loadBalancerAttributes = new LoadBalancerAttributes();
		loadBalancerAttributes.setCrossZoneLoadBalancing(crossZoneLoadBalancing);
		modifyLoadBalancerAttributesRequest.setLoadBalancerAttributes(loadBalancerAttributes);
		ModifyLoadBalancerAttributesResult modifyLoadBalancerAttributesResult = elb.modifyLoadBalancerAttributes(modifyLoadBalancerAttributesRequest);
		
		return elbDNS;
	}


	private static String newSecurityGroup(String ELBSecurityGroup) {
		
		CreateSecurityGroupRequest csgr = new CreateSecurityGroupRequest();
		csgr.withGroupName(ELBSecurityGroup).withDescription("All trafic");//.withDescription("My security group");
		CreateSecurityGroupResult createSecurityGroupResult = ec2.createSecurityGroup(csgr);
		IpPermission ipPermission = new IpPermission();
		ipPermission.withIpRanges("0.0.0.0/0").withIpProtocol("tcp").withFromPort(0).withToPort(65535);
		AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest = new AuthorizeSecurityGroupIngressRequest();
		authorizeSecurityGroupIngressRequest.withGroupName(ELBSecurityGroup).withIpPermissions(ipPermission);
		ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
		return createSecurityGroupResult.getGroupId();
	}


	private static void initialize() throws IOException {
		//Load the Properties File with AWS Credentials
		Properties properties = new Properties();
		properties.load(project2_2.class.getResourceAsStream("/AwsCredentials.properties"));
		bawsc = new BasicAWSCredentials(properties.getProperty("accessKey"), properties.getProperty("secretKey"));
		//Create an Amazon EC2 Client
		ec2 = new AmazonEC2Client(bawsc);
		elb = new AmazonElasticLoadBalancingClient(bawsc);
		autoScaling = new AmazonAutoScalingClient(bawsc);
		cloudWatch = new AmazonCloudWatchClient(bawsc);
		
	}
	
	
	private static void submitPassword(String lgDNS, String password) throws InterruptedException, IOException {
		int response;
		//args[0] is submission password 
		URL lgURL = new URL("http://" + lgDNS + "/password?passwd=" + password);
		HttpURLConnection lgConn = (HttpURLConnection) lgURL.openConnection();
		lgConn.setRequestMethod("GET");
		while ((response = lgConn.getResponseCode()) != 200){
			System.out.println("load generator response" + response);
			Thread.sleep(1000);
		}
		System.out.println("Entered password successfully!");
		
	}
	
	
	private static String getLGdns(String lgID) throws InterruptedException, IOException {
		DescribeInstancesRequest describerequestlg = new DescribeInstancesRequest().withInstanceIds(lgID);
		DescribeInstancesResult describeInstanceslg = ec2.describeInstances(describerequestlg);
		Instance loadGenerator = describeInstanceslg.getReservations().get(0).getInstances().get(0);
		String lgDNS = loadGenerator.getPublicDnsName();
		System.out.println("Load Generator is running, DNS:" + lgDNS);
		
		System.out.println("sleep for 60s");
		Thread.sleep(SLEEP_CYCLE);
		while (!checkstatus(lgID)){
			Thread.sleep(5000);
		}

		return lgDNS;
	}
	
	
	private static String launchLG(RunInstancesRequest runLoadGeneratorRequest, Tag tag) throws InterruptedException {
		
		RunInstancesResult runLoadGeneratorResult = ec2.runInstances(runLoadGeneratorRequest);
		Instance loadGenerator=runLoadGeneratorResult.getReservation().getInstances().get(0);
		String lgID = loadGenerator.getInstanceId();
		//instances.add(lgID);
		CreateTagsRequest createTagsRequest = new CreateTagsRequest();
		createTagsRequest.withResources(lgID).withTags(new Tag("Project","2.2"));
		ec2.createTags(createTagsRequest);
		System.out.println("Just launched a Load Generator with ID:" + lgID);
		do{
            System.out.println("Sleep for 60 seconds.");
            Thread.sleep(SLEEP_CYCLE);
		}while (!isRunning(lgID));
		
		return lgID;
	}
	/*
	 * get a data center DNS and wait for it being ready
	 */
	private static String getDCdns(String dcID) throws InterruptedException, IOException{
		while (!checkstatus(dcID)){
			Thread.sleep(5000);
		}
		DescribeInstancesRequest describerequestdc = new DescribeInstancesRequest().withInstanceIds(dcID);
		DescribeInstancesResult describeInstancesdc = ec2.describeInstances(describerequestdc);
		Instance dataCenter = describeInstancesdc.getReservations().get(0).getInstances().get(0);
		String dcDNS = dataCenter.getPublicDnsName();
		System.out.println("Data Center is running, DNS:" + dcDNS);

		URL dcURL = new URL("http://" + dcDNS + "/lookup/random");
		HttpURLConnection dcConn = (HttpURLConnection) dcURL.openConnection();
		dcConn.setRequestMethod("GET");
		
		int response;
		while ((response = dcConn.getResponseCode()) != 200){
			System.out.println("data center response" + response);
			Thread.sleep(1000);
		}
		System.out.println("Data center ready!");
		return dcDNS;
	}
	
	
	
	/*
	 * launch a data center and wait for it running
	 */
	private static String launchDC(RunInstancesRequest runDataCenterRequest, Tag tag) throws InterruptedException{
		RunInstancesResult runDataCenterResult = ec2.runInstances(runDataCenterRequest);
		Instance dataCenter=runDataCenterResult.getReservation().getInstances().get(0);
		String dcID = dataCenter.getInstanceId();
		CreateTagsRequest createTagsRequest = new CreateTagsRequest();
		instances.add(dcID);
		createTagsRequest.withResources(dcID).withTags(tag);
		ec2.createTags(createTagsRequest);
		System.out.println("Just launched a Data Center with ID:" + dcID);
		do{
            System.out.println("Sleep for 60 seconds.");
            Thread.sleep(SLEEP_CYCLE);
		}while (!isRunning(dcID));
		return dcID;
	}
	
	
	private static void addInstanceToELB(String dcID){
		List<com.amazonaws.services.elasticloadbalancing.model.Instance> DCs = new ArrayList<com.amazonaws.services.elasticloadbalancing.model.Instance>();
		DCs.add(new com.amazonaws.services.elasticloadbalancing.model.Instance(dcID));
        RegisterInstancesWithLoadBalancerRequest register =new RegisterInstancesWithLoadBalancerRequest();
        register.setLoadBalancerName(ELBname);
        register.setInstances(DCs);
        RegisterInstancesWithLoadBalancerResult registerWithLoadBalancerResult= elb.registerInstancesWithLoadBalancer(register);
        System.out.println("Added" + dcID + "to ELB!");
	}

	private static void updateASG(){
		UpdateAutoScalingGroupRequest updateAutoScalingGroupRequest = new UpdateAutoScalingGroupRequest();
		updateAutoScalingGroupRequest.setAutoScalingGroupName(ASGname);
		updateAutoScalingGroupRequest.setMinSize(1);
		updateAutoScalingGroupRequest.setMaxSize(3);
		updateAutoScalingGroupRequest.withDesiredCapacity(2);
		autoScaling.updateAutoScalingGroup(updateAutoScalingGroupRequest);
		
	}
	
		/*
	 * Create a new Auto Scaling Group
	 * Reference: http://marksdevserver.com/2013/03/11/setting-autoscaling-java/
	 */
	private static void myAutoScalingGroup(ArrayList<String> securityGroups, ArrayList<com.amazonaws.services.autoscaling.model.Tag> ASGtags){
		// AutoScailing

		
		/*
		 * launch configuration
		 */
		CreateLaunchConfigurationRequest lcRequest = new CreateLaunchConfigurationRequest();
        lcRequest.setLaunchConfigurationName(launchConfigName);
        lcRequest.setImageId(dcAMIid);
        lcRequest.setInstanceType(DCinstanceType);
        /**
         * EC2 security groups use the friendly name
         * VPC security groups use the identifier
         */
       
        lcRequest.setSecurityGroups(securityGroups);

        InstanceMonitoring monitoring = new InstanceMonitoring();
        monitoring.setEnabled(Boolean.TRUE);
        lcRequest.setInstanceMonitoring(monitoring);
        autoScaling.createLaunchConfiguration(lcRequest);
		
        /*
         * create auto scaling group
         */
        
        CreateAutoScalingGroupRequest asgRequest = new CreateAutoScalingGroupRequest();
        asgRequest.setAutoScalingGroupName(ASGname);
        asgRequest.setLaunchConfigurationName(launchConfigName); 
        
        ArrayList<String> avZones = new ArrayList<String>();
        avZones.add(avZone);
        asgRequest.setAvailabilityZones(avZones);
        asgRequest.setMinSize(1); 
        asgRequest.setMaxSize(1);
        asgRequest.setTags(ASGtags);
        ArrayList<String> elbs = new ArrayList<String>();
        elbs.add(ELBname);
        asgRequest.setLoadBalancerNames(elbs);

        asgRequest.setHealthCheckType("ELB");
        asgRequest.setHealthCheckGracePeriod(150);
        asgRequest.setDefaultCooldown(60);
        //asgRequest.setVPCZoneIdentifier("172.31.48.0/20");
        
        autoScaling.createAutoScalingGroup(asgRequest);

        /*
         * policy & Alarm
         */
        myScaleUp();
        myScaleDown();
        
        

	}
	
	private static void myScaleDown() {
        /*
         * policy
         */
        PutScalingPolicyRequest request = new PutScalingPolicyRequest();
        request.setAutoScalingGroupName(ASGname);
        request.setPolicyName("ScaleDown"); 
        request.setScalingAdjustment(-1); 
        request.setAdjustmentType("ChangeInCapacity");
        request.setCooldown(60);
        PutScalingPolicyResult upPolocyResult = autoScaling.putScalingPolicy(request);
        String downArn = upPolocyResult.getPolicyARN(); 
        
        
        /*
         * alarm
         */

        // Scale Down
        PutMetricAlarmRequest downRequest = new PutMetricAlarmRequest();
        downRequest.setAlarmName("ScaleDown");
        downRequest.setMetricName("CPUUtilization");

        ArrayList<Dimension> dimensions = new ArrayList<Dimension>();
        Dimension dimension = new Dimension();
        dimension.setName("AutoScalingGroupName");
        dimension.setValue(ASGname);
        dimensions.add(dimension);
        downRequest.setDimensions(dimensions);
        
        downRequest.setNamespace("AWS/EC2");
        downRequest.setComparisonOperator(ComparisonOperator.LessThanThreshold);
        downRequest.setStatistic(Statistic.Average);
        downRequest.setUnit(StandardUnit.Percent);
        downRequest.setThreshold(25d);
        downRequest.setPeriod(300);
        downRequest.setEvaluationPeriods(1);
        ArrayList<String> actions = new ArrayList<String>();
        actions.add(downArn);
        downRequest.setAlarmActions(actions);

        cloudWatch.putMetricAlarm(downRequest);
	}


	private static void myScaleUp() {
        /*
         * policy
         */
        PutScalingPolicyRequest request = new PutScalingPolicyRequest();
        request.setAutoScalingGroupName(ASGname);
        request.setPolicyName("ScaleUp"); 
        request.setScalingAdjustment(1); 
        request.setAdjustmentType("ChangeInCapacity");
        request.setCooldown(60);
        PutScalingPolicyResult upPolocyResult = autoScaling.putScalingPolicy(request);
        String upArn = upPolocyResult.getPolicyARN(); 
        
        
        /*
         * alarm
         */

        // Scale Up
        PutMetricAlarmRequest upRequest = new PutMetricAlarmRequest();
        upRequest.setAlarmName("ScaleUp");
        upRequest.setMetricName("CPUUtilization");

        ArrayList<Dimension> dimensions = new ArrayList<Dimension>();
        Dimension dimension = new Dimension();
        dimension.setName("AutoScalingGroupName");
        dimension.setValue(ASGname);
        dimensions.add(dimension);
        upRequest.setDimensions(dimensions);

        upRequest.setNamespace("AWS/EC2");
        upRequest.setComparisonOperator(ComparisonOperator.GreaterThanThreshold);
        upRequest.setStatistic(Statistic.Average);
        upRequest.setUnit(StandardUnit.Percent);
        upRequest.setThreshold(70d);
        upRequest.setPeriod(60);
        upRequest.setEvaluationPeriods(1);
        ArrayList<String> actions = new ArrayList<String>();
        actions.add(upArn); 
        upRequest.setAlarmActions(actions);

        cloudWatch.putMetricAlarm(upRequest);
		
	}


	private static void deleteSG(String sg){
		DeleteSecurityGroupRequest deleteSecurityGroupRequest = new DeleteSecurityGroupRequest(sg);
		ec2.deleteSecurityGroup(deleteSecurityGroupRequest);
		System.out.println("Security Group Deleted!");
	}
	
	
	private static boolean isRunning(String instanceID){
		
		DescribeInstancesRequest describerequest = new DescribeInstancesRequest().withInstanceIds(instanceID);
		DescribeInstancesResult describeInstance = ec2.describeInstances(describerequest);
		String state = describeInstance.getReservations().get(0).getInstances().get(0).getState().getName();
		
		//System.out.println(state);
		if (state.equals("running"))
			return true;
		return false;
	}
	
	private static void terminate() throws IOException, InterruptedException{
		//Prompt User
		//System.out.printf("\nTerminate Instance? (y/N): ");
		//BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
		//String userResponse = bufferRead.readLine();
		//if(userResponse.toLowerCase().equals("y")){
		
			//get all running instances
		DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest();
		DescribeInstancesResult describeInstancesResult = ec2.describeInstances(describeInstancesRequest);
		for (Reservation reservation : describeInstancesResult.getReservations()){
			for (Instance instance : reservation.getInstances()){
				if (instance.getImageId().equalsIgnoreCase(dcAMIid)){// data center
					instances.add(instance.getInstanceId());
				}
			}
		}
			//Terminate Instance
		if (!instances.isEmpty()){
			TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest();

			terminateInstancesRequest.setInstanceIds(instances);
			ec2.terminateInstances(terminateInstancesRequest);
			System.out.println("Instances terminated!");
		}	
		DeleteLoadBalancerRequest deleteLoadBalancerRequest = new DeleteLoadBalancerRequest(ELBname);
		elb.deleteLoadBalancer(deleteLoadBalancerRequest);
		System.out.println("ELB Deleted!");

			
		//}
	}
	
	private static void delASG() {
		DeleteAutoScalingGroupRequest deleteAutoScalingGroupRequest = new DeleteAutoScalingGroupRequest();
		deleteAutoScalingGroupRequest.setAutoScalingGroupName(ASGname);
		autoScaling.deleteAutoScalingGroup(deleteAutoScalingGroupRequest);
		System.out.println("ASG Deleted!");
	}


	private static boolean checkstatus(String id) throws IOException{
		DescribeInstanceStatusRequest describeInstanceRequest = new DescribeInstanceStatusRequest().withInstanceIds(id);
		DescribeInstanceStatusResult describeInstanceResult = ec2.describeInstanceStatus(describeInstanceRequest);
		String status = describeInstanceResult.getInstanceStatuses().get(0).getInstanceStatus().getStatus();
		//System.out.println(status);
		return status.equalsIgnoreCase("OK");
	}
	
}
