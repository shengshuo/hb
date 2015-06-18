package hbaseHelper;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HexStringTest {
	public static void main(String[] args) throws NoSuchAlgorithmException {
		byte value = (byte) 0x0f;//(byte) 0xfe; //Signed for -2 and unsigned for 254
		//int result = value;// & 0xff;
		System.out.println(value);
		System.out.println(String.format("%02x", value));
		//System.out.println((byte)0xff);
		StringBuffer hexString = new StringBuffer();
		hexString.append("0" + Integer.toHexString((0xFF & value)));
		
		System.out.println(hexString.toString());
//		System.out.println(value & 0xff);
		//System.out.println(result);
		//		String hexStr = "";
//		String inStr ="";
//		hexStr = Integer.toHexString(16);
//		System.out.println(hexStr);
//		System.out.println("done");
		System.out.println();
		String hstr = getHashStringTest("abcde");
		System.out.println(hstr);
	}
	
	public static String getHashStringTest(String beHashedStr) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("SHA-256");		
		md.update(beHashedStr.getBytes());
		byte[] hashBytes = md.digest();
		
		StringBuffer hexString = new StringBuffer();
		for (int i = 0; i < hashBytes.length; i++) {
			System.out.println( ">> "+ hashBytes[i]);
			
			if ((0xff & hashBytes[i]) < 0x10) {
				hexString.append("0" + Integer.toHexString((0xFF & hashBytes[i])));
				
			} else {
				hexString.append(Integer.toHexString(0xFF & hashBytes[i]));
			}
			System.out.println("Rslt:["+ hexString+"]\n"); 
		}
		
		return hexString.toString();
	}	
	
	public static String getHashString(String beHashedStr) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("SHA-256");		
		md.update(beHashedStr.getBytes());
		byte[] hashBytes = md.digest();
		
		StringBuffer hexString = new StringBuffer();
		for (int i = 0; i < hashBytes.length; i++) {
			if ((0xff & hashBytes[i]) < 0x10) {
				hexString.append("0" + Integer.toHexString((0xFF & hashBytes[i])));
			} else {
				hexString.append(Integer.toHexString(0xFF & hashBytes[i]));
			}
		}
		return hexString.toString();
	}
}
