/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package com.mycompany.ha1;
import java.util.*;

public class HA1 {

    public static void main(String[] args) {
        Scanner sc=new Scanner(System.in);
		String str=sc.nextLine();
		
		char arr[]=str.toCharArray();
		int new_arr[]=new int[256];
		for(int i=0;i<str.length();i++) {
			new_arr[arr[i]]++;
		}
		for(int i=0;i<str.length();i++) {
			if(new_arr[arr[i]]==1) {
				System.out.println(arr[i]);
			break;}
		}

    }
}
