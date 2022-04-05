package sosp.main;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;

public class Test2{

    public static void main(String[] args) throws Exception{
        double size = 10;
        int mapper = 10;
        int reducer = 2;
        String space = " ";
        StringBuilder sb = new StringBuilder();
        sb.append(1).append(space);
        sb.append(1000).append(space);
        sb.append(mapper).append(space);
        for(int i = 0; i < mapper; i ++){
            sb.append(1).append(space);
        }
        sb.append(reducer).append(space);
        double[] rand = new double[reducer];
        for(int i = 0; i < reducer; i ++) {
            rand[i] = Math.random();
        }
        Arrays.sort(rand);
        for (int i = 0; i < reducer; i ++) {
            if(i == 0) {
                sb.append(1).append(":").append(size*rand[i]*1000).append(space);
            }
            else{
                sb.append(1).append(":").append(size*(rand[i] - rand[i - 1])*1000).append(space);
            }
        }
        System.out.println(sb);
    }
}
