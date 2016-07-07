import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

public class RunInstance {
	//Data and parameters
	private static final long SLEEP_CYCLE = 60000;
	private static String securityGroup = "All";
	private static String keyName = "project0demo";
	private static String instanceType = "m3.medium";
	
	
	//Methods
	public static void main(String[] args) throws Exception{
		//Load the Properties File with AWS Credentials
		Properties properties = new Properties();
		properties.load(RunInstance.class.getResourceAsStream("/AwsCredentials.properties"));
		BasicAWSCredentials bawsc = new BasicAWSCredentials(properties.getProperty("accessKey"), properties.getProperty("secretKey"));
		//Create an Amazon EC2 Client
		AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);
		//Create Instance Request
		RunInstancesRequest runLoadGeneratorRequest = new RunInstancesRequest();
		RunInstancesRequest runDataCenterRequest = new RunInstancesRequest();
		//Configure Instance Request
		runLoadGeneratorRequest.withImageId("ami-4389fb26")
		.withInstanceType(instanceType)
		.withMinCount(1)
		.withMaxCount(1)
		.withKeyName(keyName)
		.withSecurityGroups(securityGroup);
		
		runDataCenterRequest.withImageId("ami-abb8cace")
		.withInstanceType(instanceType)
		.withMinCount(1)
		.withMaxCount(1)
		.withKeyName(keyName)
		.withSecurityGroups(securityGroup);
		
		//Launch load generator
		List<String> instances = new ArrayList<String>();
		RunInstancesResult runLoadGeneratorResult = ec2.runInstances(runLoadGeneratorRequest);
		Instance loadGenerator=runLoadGeneratorResult.getReservation().getInstances().get(0);
		String lgID = loadGenerator.getInstanceId();
		instances.add(lgID);
		CreateTagsRequest createTagsRequest = new CreateTagsRequest();
		createTagsRequest.withResources(lgID).withTags(new Tag("Project","2.1"));
		ec2.createTags(createTagsRequest);
		System.out.println("Just launched a Load Generator with ID:" + lgID);
		do{
            System.out.println("Sleep for 60 seconds.");
            Thread.sleep(SLEEP_CYCLE);
		}while (!isRunning(lgID, ec2));
		DescribeInstancesRequest describerequestlg = new DescribeInstancesRequest().withInstanceIds(lgID);
		DescribeInstancesResult describeInstanceslg = ec2.describeInstances(describerequestlg);
		loadGenerator = describeInstanceslg.getReservations().get(0).getInstances().get(0);
		String lgDNS = loadGenerator.getPublicDnsName();
		System.out.println("Load Generator is running, DNS:" + lgDNS);
		
		System.out.println("sleep for 60s");
		
		//Submit password
		Thread.sleep(SLEEP_CYCLE);
		int response;
		URL lgURL = new URL("http://" + lgDNS + "/password?passwd=ML9xCNya1wUBR1W5CXS8oVbMwdVzWTQF");
		HttpURLConnection lgConn = (HttpURLConnection) lgURL.openConnection();
		lgConn.setRequestMethod("GET");
		while ((response = lgConn.getResponseCode()) != 200){
			System.out.println("load generator response" + response);
			Thread.sleep(1000);
		}
		System.out.println("Entered password successfully!");
		
		
		//Launch a data center and wait for it being ready
		RunInstancesResult runDataCenterResult = ec2.runInstances(runDataCenterRequest);
		Instance dataCenter=runDataCenterResult.getReservation().getInstances().get(0);
		String dcID = dataCenter.getInstanceId();
		instances.add(dcID);
		createTagsRequest.withResources(dcID).withTags(new Tag("Project","2.1"));
		ec2.createTags(createTagsRequest);
		System.out.println("Just launched a Data Center with ID:" + dcID);
		do{
            System.out.println("Sleep for 60 seconds.");
            Thread.sleep(SLEEP_CYCLE);
		}while (!isRunning(dcID, ec2));
		DescribeInstancesRequest describerequestdc = new DescribeInstancesRequest().withInstanceIds(dcID);
		DescribeInstancesResult describeInstancesdc = ec2.describeInstances(describerequestdc);
        dataCenter = describeInstancesdc.getReservations().get(0).getInstances().get(0);
		String dcDNS = dataCenter.getPublicDnsName();
		System.out.println("Data Center is running, DNS:" + dcDNS);
		Thread.sleep(SLEEP_CYCLE);
		URL dcURL = new URL("http://" + dcDNS + "/lookup/random");
		HttpURLConnection dcConn = (HttpURLConnection) dcURL.openConnection();
		dcConn.setRequestMethod("GET");
		while ((response = dcConn.getResponseCode()) != 200){
			System.out.println("data center response" + response);
			Thread.sleep(1000);
		}
		System.out.println("Data center ready!");
		
		
		InputStreamReader inputStreamReader = new InputStreamReader(lgConn.getInputStream());
		String tempLine;
		StringBuilder result = new StringBuilder();
		URL submitDNSURL = new URL("http://" + lgDNS + "/test/horizontal?dns=" + dcDNS);
		HttpURLConnection submitDNS = (HttpURLConnection) submitDNSURL.openConnection();
		submitDNS.setRequestMethod("GET");
		while ((response = submitDNS.getResponseCode()) != 200){
			System.out.println("load generator response" + response);
			Thread.sleep(1000);
		}
		System.out.println("Submitted DNS successfully!");
		inputStreamReader = new InputStreamReader(submitDNS.getInputStream());
		BufferedReader bufferedReader2 = new BufferedReader(inputStreamReader);
		result = new StringBuilder();
		while ((tempLine = bufferedReader2.readLine()) != null){
			result.append(tempLine);
		}
		System.out.println(result);
		bufferedReader2.close();
		int index;
		int endindex;
		float totalrps = 0;
		int rpsindex;
		int dccount = 1;
		boolean flagbreak = false;
		String testlog;
		if ((index = result.toString().lastIndexOf("/log?name=test.")) != -1){
			endindex = result.toString().indexOf(".log", index);
			testlog = result.toString().substring(index+15, endindex);
			System.out.println(testlog);
			
			while (totalrps < 4000 && dccount < 9){
				
				//Launch a data center and wait for it being ready
				runDataCenterResult = ec2.runInstances(runDataCenterRequest);
				dataCenter=runDataCenterResult.getReservation().getInstances().get(0);
				dcID = dataCenter.getInstanceId();
				instances.add(dcID);
				createTagsRequest.withResources(dcID).withTags(new Tag("Project","2.1"));
				ec2.createTags(createTagsRequest);
				System.out.println("Just launched a Data Center with ID:" + dcID);
				do{
		            System.out.println("Sleep for 60 seconds.");
		            Thread.sleep(SLEEP_CYCLE);
				}while (!isRunning(dcID, ec2));
				describerequestdc = new DescribeInstancesRequest().withInstanceIds(dcID);
				describeInstancesdc = ec2.describeInstances(describerequestdc);
		        dataCenter = describeInstancesdc.getReservations().get(0).getInstances().get(0);
				dcDNS = dataCenter.getPublicDnsName();
				System.out.println("Data Center is running, DNS:" + dcDNS);
				Thread.sleep(SLEEP_CYCLE);
				dcURL = new URL("http://" + dcDNS + "/lookup/random");
				dcConn = (HttpURLConnection) dcURL.openConnection();
				dcConn.setRequestMethod("GET");
				while ((response = dcConn.getResponseCode()) != 200){
					System.out.println("data center response" + response);
					Thread.sleep(1000);
				}
				System.out.println("Data center ready!");

				
				
				
				URL addDCURL = new URL("http://" + lgDNS + "/test/horizontal/add?dns=" + dcDNS);
				HttpURLConnection addDC = (HttpURLConnection) addDCURL.openConnection();
				addDC.setRequestMethod("GET");
				while ((response = addDC.getResponseCode()) != 200){
					if (response == 400)
						flagbreak = true;
					System.out.println("data center response" + response);
					Thread.sleep(1000);
				}
				if (flagbreak)
					break;
				
				dccount += 1;
				System.out.println("There are total" + dccount + "data centers added.");
				
				
				System.out.println("sleep for 100s");
				Thread.sleep(SLEEP_CYCLE);
			
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
					if (tempLine.lastIndexOf("[Minute ") != -1)
						totalrps = 0;
					if ((rpsindex = tempLine.lastIndexOf(".amazonaws.com=")) != -1)
						totalrps += Float.parseFloat(tempLine.substring(rpsindex+15));
					//result.append(tempLine);
				}
				bufferedReader3.close();
				System.out.println(totalrps);
	
			}
		}
		
		
		
		
		
		
		
		
		/* Wait until both instances are running
		 * References:
		 *  http://stackoverflow.com/questions/12065059/from-the-aws-java-api-how-can-i-tell-when-my-ebs-snapshot-has-been-created
		 *  http://stackoverflow.com/questions/9241584/get-public-dns-of-amazon-ec2-instance-from-java-api
		 */
		/*
		boolean flagrunning = true;
        do
        {
        	flagrunning = true;
            System.out.println("Sleep for 60 seconds.");
            Thread.sleep(SLEEP_CYCLE);
    		DescribeInstancesRequest describerequest = new DescribeInstancesRequest().withInstanceIds(lgID, dcID);
    		DescribeInstancesResult describeInstances = ec2.describeInstances(describerequest);
    		List<String> states = new ArrayList<String>();
    		for (Reservation reservation : describeInstances.getReservations()){
    			for (Instance instance : reservation.getInstances()){
    				states.add(instance.getState().getName());
    			}
    		}
    		System.out.println(states);

    		for (String state : states){
    			flagrunning = flagrunning && state.equals("running");
    		}
        } while (!flagrunning);
        */
        //GetPublicDNS
		

		/* Read HTTP response
		 * Reference: 
		 *  http://stackoverflow.com/questions/1485708/how-do-i-do-a-http-get-in-java
		 *  http://stackoverflow.com/questions/4205980/java-sending-http-parameters-via-post-method-easily
		 *  http://stackoverflow.com/questions/3324717/sending-http-post-request-in-java
		 */

		
		//data center
		

		
		// load generator

			//byte[] subPass = new String("?passwd=ML9xCNya1wUBR1W5CXS8oVbMwdVzWTQF").getBytes(StandardCharsets.UTF_8);
			
	        /*
			HttpClient httpclient = HttpClients.createDefault();
			HttpPost httppost = new HttpPost("http://" + lgDNS + "/password");

			// Request parameters and other properties.
			List<NameValuePair> params = new ArrayList<NameValuePair>(1);
			params.add(new BasicNameValuePair("passwd", "ML9xCNya1wUBR1W5CXS8oVbMwdVzWTQF"));
			httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

			//Execute and get the response.
			HttpResponse response = httpclient.execute(httppost);
			HttpEntity entity = response.getEntity();

			byte[] b = new byte[1000];
			if (entity != null) {
			    InputStream instream = entity.getContent();
			    try {
			        instream.read(b);
			        System.out.println(b);
			    } finally {
			        instream.close();
			    }
			}
			*/
			




			/*
			BufferedReader bufferedReader1 = new BufferedReader(inputStreamReader);

			while ((tempLine = bufferedReader1.readLine()) != null){
				result.append(tempLine);
			}
			System.out.println(result);
			bufferedReader1.close();
			*/
			//Thread.sleep(5000);

			//Thread.sleep(5000);
		/*			
			//read response1
			InputStreamReader inputStreamReader = new InputStreamReader(submitPassword.getInputStream());
			BufferedReader bufferedReader1 = new BufferedReader(inputStreamReader);
			String tempLine;
			StringBuilder result = new StringBuilder();
			while ((tempLine = bufferedReader1.readLine()) != null){
				result.append(tempLine);
			}
			System.out.println(result);
			bufferedReader1.close();
			
			//read response2
			inputStreamReader = new InputStreamReader(submitDNS.getInputStream());
			BufferedReader bufferedReader2 = new BufferedReader(inputStreamReader);
			result = new StringBuilder();
			while ((tempLine = bufferedReader2.readLine()) != null){
				result.append(tempLine);
			}
			System.out.println(result);
			bufferedReader2.close();
		*/


		//terminate instances
		terminate(instances, ec2);


		
	}
	
	
	private static boolean isRunning(String instanceID, AmazonEC2Client ec2){
		
		DescribeInstancesRequest describerequest = new DescribeInstancesRequest().withInstanceIds(instanceID);
		DescribeInstancesResult describeInstance = ec2.describeInstances(describerequest);
		String state = describeInstance.getReservations().get(0).getInstances().get(0).getState().getName();
		
		//System.out.println(state);
		if (state.equals("running"))
			return true;
		return false;
	}
	
	private static void terminate(List<String> instances, AmazonEC2Client ec2) throws IOException{
		//Prompt User
		System.out.printf("\nTerminate Instance? (y/N): ");
		BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
		String userResponse = bufferRead.readLine();
		if(userResponse.toLowerCase().equals("y")){
			//Terminate Instance
			TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest();

			terminateInstancesRequest.setInstanceIds(instances);
			
			ec2.terminateInstances(terminateInstancesRequest);
			
		}
	}
	
	private static StringBuffer sendHttpRequest(String urlstr) throws IOException{
		URL url = new URL(urlstr);
		String line = null;
		StringBuffer result = new StringBuffer();
		URLConnection connection = url.openConnection();
		InputStream response = connection.getInputStream();
		BufferedReader reader = new BufferedReader(new InputStreamReader(response));
		while ((line = reader.readLine()) != null){
			result.append(line + '\r');
		}
		return result;
	}
	
}
