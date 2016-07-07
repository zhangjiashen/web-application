

import java.math.BigInteger;
import java.lang.Math;
public class Caesar {
    private static String key_X_string = "8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773";
    private static BigInteger key_X = new BigInteger(key_X_string);
    
    public static String decode(String secret_message, String key_string) {
        //char[] intermediate_message = new char[secret_message.length()];
    	BigInteger key_XY = new BigInteger(key_string);
    	BigInteger key_Y = key_XY.divide(key_X);
        String key_Y_string = key_Y.toString();
        StringBuilder ret = new StringBuilder(secret_message.length());
        int key_Z = 1 + Integer.parseInt(key_Y_string.substring(key_Y_string.length() - 2)) % 25;
        int arrlen = (int)Math.sqrt((double)secret_message.length());
        int i;
        int j; 
        //int count=0;
        for (i=0; i<arrlen; i++){
            for (j=0; j<=i; j++){
                ret.append((char)('A' + (secret_message.charAt(j*arrlen+i-j) + 26 - key_Z - 'A') % 26));
                //count++;
            }
        }
        int k=1;
        for (i=arrlen; i<2*arrlen-1; i++){
            for (j=k; j<arrlen; j++){
            	ret.append((char)('A' + (secret_message.charAt(j*arrlen+i-j) + 26 - key_Z - 'A') % 26));
                //count++;
            }
            k++;
        }
        
        
        
        //String ret = new String(intermediate_message);
        
        return ret.toString();
    }


    public static void main (String[] args) {
    	
        System.out.println(decode("YUUSYEMKBFNKBWRGDPKMUUURHSJKSKMZMRREEGHPDYZDMQGHTYUCJWYLIPSLNCPO", "34442366983873288747750680682481187949445162517128606954949128128031393675537463411833201701753422430676672997"));
        //System.out.println(decodetest("HELLOWORK"));
    }
}