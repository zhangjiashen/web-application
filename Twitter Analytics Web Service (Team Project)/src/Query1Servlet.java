package ccTeam;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.*;
import java.math.BigDecimal;
import java.util.Calendar;

/**
 * Servlet implementation class Query1Servlet
 */
@WebServlet("/q1")
public class Query1Servlet extends HttpServlet {
 
  private static final long serialVersionUID = 1L;
  private static final String key_X_string = "8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773";
  private static final String teamId = "Yamete";
  private static final String teamAWSAccountId = "148723206667";
  private static BigDecimal key_X = new BigDecimal(key_X_string);
  
  public void init() throws ServletException
  {
      // Initialization
      
  }

  
  public static String decode(String secret_message, String key_string) {
      char[] intermediate_message = new char[secret_message.length()+1];
      BigDecimal key_XY = new BigDecimal(key_string);
      BigDecimal key_Y = key_XY.divide(key_X);
      String key_Y_string = key_Y.toString();
      int key_Z = 1 + Integer.parseInt(key_Y_string.substring(key_Y_string.length() - 2)) % 25;
      //char keyz = Character.toChars(key_Z)[0];

      int arrlen = (int)Math.sqrt((double)secret_message.length());
      //System.out.println(arrlen);
      char[][]array=new char[arrlen][arrlen];
             
      int i;
      int j; 
      for (i=0; i<arrlen; i++){
          for (j=0; j<arrlen; j++){
              array[i][j] = secret_message.charAt(i*arrlen+j);
          }
      }
      
      

      int count=0;
      for (i=0; i<arrlen; i++){
          for (j=0; j<=i; j++){
              intermediate_message[count] = array[j][i-j];
              count++;
          }
      }
      int k=1;
      for (i=arrlen; i<2*arrlen-1; i++){
          for (j=k; j<arrlen; j++){
          	intermediate_message[count] = array[j][i-j];
              count++;
          }
          k++;
      }


      for (i = 0; i < secret_message.length(); i++) {
          intermediate_message[i] = Character.toChars('A' + (intermediate_message[i] + 26 - key_Z - 'A') % 26)[0];
      }
      
      intermediate_message[count] = 0;
      
      
      String ret = new String(intermediate_message);
      return ret;
  }
  
  public void doGet(HttpServletRequest request,
                    HttpServletResponse response)
            throws ServletException, IOException
  {
      // 设置响应内容类型
      response.setContentType("text/html");

      // 实际的逻辑是在这里
      String message = decode(request.getParameter("key"), request.getParameter("message"));
      PrintWriter out = response.getWriter();
      out.println(this.generateResponse(message));
//      out.println(request.getParameter("key") + '\n' + request.getParameter("message"));
      out.close();
  }
  
  private String generateResponse(String message) {
//	  TEAMID,TEAM_AWS_ACCOUNT_ID\n
//	  yyyy-MM-dd HH:mm:ss\n
//	  \n
//	  TeamCoolCloud,1234-0000-0001
//	  2004-08-15 16:23:42
//	  HELLOWORK
	  
	  Calendar calendar = Calendar.getInstance();
	  StringBuffer responseBuffer = new StringBuffer();
	  responseBuffer
	  .append(teamId)
	  .append(',')
	  .append(teamAWSAccountId)
	  .append("\n")
	  .append(calendar.get(Calendar.YEAR))
	  .append('-')
	  .append(String.format("%02d", calendar.get(Calendar.MONTH)))
	  .append('-')
	  .append(String.format("%02d", calendar.get(Calendar.DATE)))
	  .append(' ')
	  .append(String.format("%02d", calendar.get(Calendar.HOUR)))
	  .append(':')
	  .append(String.format("%02d", calendar.get(Calendar.MINUTE)))
	  .append(':')
	  .append(String.format("%02d", calendar.get(Calendar.SECOND)))
	  .append("\n")
	  .append(message);
	  
	  return responseBuffer.toString();
	  
  }
}
