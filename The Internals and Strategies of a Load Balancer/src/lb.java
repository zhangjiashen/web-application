import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;

/* Reference; 
 * https://www.caveofprogramming.com/java/java-file-reading-and-writing-files-in-java.html
 * */
public class LoadBalancer {
	private final int CYCLE = 40;// Cycle to compute weights
	private final int HEALTHCYCLE = 1000;//second
	private final int NORMAL = 5;// Normalization factor
	private static final int THREAD_POOL_SIZE = 4;
	private final ServerSocket socket;
	private final DataCenterInstance[] instances;
	public Tag tag = new Tag("Project","2.2");
	private int i;
	public long count;
	public URL[] getCPUurl;
	public URL[] checkurl;
	public HttpURLConnection[] getCPU;
	public HttpURLConnection[] checkHealth;
	public InputStreamReader[] inputStreamReader;
	public BufferedReader[] bufferedReader;
	public float[] CPUUtilization;// This is actually free CPU percent
	public int[] seq;//Normalized cumsum of weights, Used for allocating request
	public String tempLine;
	public String tmpstr;
	public float temp;
	public int mindc;
	public boolean ishealthy[];
	private String dcID[];
	private String DCinstanceType = "m3.medium";
	private String dcAMIid = "ami-ed80c388";
	private String securityGroup = "All";
	private String keyName = "project0demo";	
	private BasicAWSCredentials bawsc;
	private AmazonEC2Client ec2;
	private String num2str[];
	
	RunInstancesRequest runDataCenterRequest = new RunInstancesRequest();
	
	public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) throws IOException {
		this.socket = socket;
		this.instances = instances;
		i=0;
		getCPUurl = new URL[3];
		checkurl = new URL[3];
		getCPU = new HttpURLConnection[3];
		checkHealth = new HttpURLConnection[3];
		inputStreamReader = new InputStreamReader[3];
		bufferedReader = new BufferedReader[3];
		CPUUtilization = new float[3];
		seq = new int[3];
		dcID = new String[3];
		ishealthy = new boolean[3];
		for (int j=0; j<3; j++){
			ishealthy[j]=true;
		}
		num2str = new String[3];
		num2str[0] = "first";
		num2str[1] = "second";
		num2str[2] = "third";
		
		getCPUurl[0] = new URL("http://ec2-52-91-246-90.compute-1.amazonaws.com:8080/info/cpu");
		getCPUurl[1] = new URL("http://ec2-54-152-59-216.compute-1.amazonaws.com:8080/info/cpu");
		getCPUurl[2] = new URL("http://ec2-54-209-198-73.compute-1.amazonaws.com:8080/info/cpu");
		checkurl[0] = new URL("http://ec2-52-91-246-90.compute-1.amazonaws.com/lookup/random");
		checkurl[1] = new URL("http://ec2-54-152-59-216.compute-1.amazonaws.com/lookup/random");
		checkurl[2] = new URL("http://ec2-54-209-198-73.compute-1.amazonaws.com/lookup/random");
		
		String accessKey;
		String secretKey;
		
        FileReader fileReader1 = new FileReader("./accessKey");
        FileReader fileReader2 = new FileReader("./secretKey");

        BufferedReader bufferedReader1 = new BufferedReader(fileReader1);
        BufferedReader bufferedReader2 = new BufferedReader(fileReader2);
        accessKey = bufferedReader1.readLine();
		secretKey = bufferedReader2.readLine();
		
		bawsc = new BasicAWSCredentials(accessKey, secretKey);
		//Create an Amazon EC2 Client
		ec2 = new AmazonEC2Client(bawsc);

		
		runDataCenterRequest.withImageId(dcAMIid)
		.withInstanceType(DCinstanceType)
		.withMinCount(1)
		.withMaxCount(1)
		.withKeyName(keyName)
		.withSecurityGroups(securityGroup);

	}

	// Complete this function
	public void start() throws IOException {
		/* Round Robin */
		ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		
		while (true){
			if(ishealthy[i]){
				Runnable requestHandler = new RequestHandler(socket.accept(), instances[i]);
				executorService.execute(requestHandler);
			}
			
			i++;
			count++;
			if (count == HEALTHCYCLE){
				healthcheck();
				count = 0;
			}
			if (i==3){
				i=0;
			}
		}
		/*
		ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		while (true) {
			// By default, it will send all requests to the first instance
			if (i%CYCLE == 0){
				cpu();
				i = 0;
			}
			Runnable requestHandler;
			if (i%NORMAL < seq[0]){
				requestHandler = new RequestHandler(socket.accept(), instances[0]);
			} else if (i%NORMAL < seq[1]){
				requestHandler = new RequestHandler(socket.accept(), instances[1]);
			} else{
				requestHandler = new RequestHandler(socket.accept(), instances[2]);
			}
			
			i++;
			executorService.execute(requestHandler);
		}
		*/
	}
	
	public void cpu() throws IOException {
		
		
		
		//int floatindex;
		int j;
		
		for (j = 0; j < 3; j++){
			getCPU[j] = (HttpURLConnection) getCPUurl[j].openConnection();
			getCPU[j].setConnectTimeout(300);
			getCPU[j].connect();
			if ( getCPU[j].getResponseCode() == 200){
				
				inputStreamReader[j] = new InputStreamReader(getCPU[j].getInputStream());
				bufferedReader[j] = new BufferedReader(inputStreamReader[j]);
				tempLine = bufferedReader[j].readLine();
				if (tempLine!=null && (!tempLine.isEmpty())){
				//System.out.println(tempLine);// from 70
				//floatindex = tempLine.lastIndexOf('b');
					tmpstr = tempLine.substring(70, tempLine.length() - 14);
					if(!tmpstr.isEmpty()){
						temp = Float.parseFloat(tmpstr);
						CPUUtilization[j] = 100 - temp;// free CPU
					}
				}
				
				bufferedReader[j].close();
			}
			getCPU[j].disconnect();
		}
		
		float sum = CPUUtilization[0] + CPUUtilization[1] + CPUUtilization[2];
		
		
		seq[0] = Math.round( (CPUUtilization[0] * NORMAL/sum));
		seq[1] = seq[0] + Math.round((CPUUtilization[1] * NORMAL/sum));
		seq[2] = NORMAL;
		/*
		for (j = 1; j < 3; j++){
			seq[j] = seq[j-1] + (int) ((1 - CPUUtilization[j])/10);			
		}
		*/
		
		//return ret;
	}
	
	public void healthcheck() throws IOException {
		int j;
		for (j = 0; j < 3; j++){
			if (!ishealthy[j]){
				if (checkstatus(dcID[j])){
					String dcDNS = getDCdns(dcID[j]);
					if (!dcDNS.isEmpty()){
						getCPUurl[j] = new URL("http://" + dcDNS + ":8080/info/cpu");
						checkurl[j] = new URL("http://" + dcDNS + "/lookup/random");
						instances[j] = new DataCenterInstance(num2str[j] + "_instance", "http://" + dcDNS);
						ishealthy[j] = true;
					}
				}
			}
		}
		
		
		for (j = 0; j < 3; j++){
			if (ishealthy[j]){
				checkHealth[j] = (HttpURLConnection) getCPUurl[j].openConnection();
				checkHealth[j].setConnectTimeout(300);
				checkHealth[j].connect();
				if ( checkHealth[j].getResponseCode() != 200){
					ishealthy[j] = false;
					dcID[j] = launchDC();
			}
				
			checkHealth[j].disconnect();
			
			}
		}

	}
	
	private boolean checkstatus(String id) throws IOException{
		DescribeInstanceStatusRequest describeInstanceRequest = new DescribeInstanceStatusRequest().withInstanceIds(id);
		DescribeInstanceStatusResult describeInstanceResult = ec2.describeInstanceStatus(describeInstanceRequest);
		String status = describeInstanceResult.getInstanceStatuses().get(0).getInstanceStatus().getStatus();
		//System.out.println(status);
		return status.equalsIgnoreCase("OK");
	}
	
	private String getDCdns(String dcID) throws InterruptedException, IOException{
		DescribeInstancesRequest describerequestdc = new DescribeInstancesRequest().withInstanceIds(dcID);
		DescribeInstancesResult describeInstancesdc = ec2.describeInstances(describerequestdc);
		Instance dataCenter = describeInstancesdc.getReservations().get(0).getInstances().get(0);
		String dcDNS = dataCenter.getPublicDnsName();
		System.out.println("Data Center is running, DNS:" + dcDNS);
		return dcDNS;
	}
	
	private String launchDC() throws InterruptedException{
		RunInstancesResult runDataCenterResult = ec2.runInstances(runDataCenterRequest);
		Instance dataCenter=runDataCenterResult.getReservation().getInstances().get(0);
		String dcID = dataCenter.getInstanceId();
		CreateTagsRequest createTagsRequest = new CreateTagsRequest();
		createTagsRequest.withResources(dcID).withTags(tag);
		ec2.createTags(createTagsRequest);
		System.out.println("Just launched a Data Center with ID:" + dcID);
		return dcID;
	}
	
}
