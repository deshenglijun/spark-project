package com.desheng.bigdata;

import java.util.Random;

/**
 * 水塘抽样算法
 *   从一个非常大的集合中，随机找到N个值。
 */
public class PondSampleTest {
    public static void main(String[] args) {
        int N = 10000;
        int[] S = new int[N];
        for(int i = 0; i < N; i++) {
            S[i] = i;
        }
        int K = 10;
        int[] R = new int[K];
        for(int i = 0; i < K; i++) {
            R[i] = S[i];
        }

        //
        Random random = new Random();
        for(int i = K; i < N; i++) {
            int j = random.nextInt(i);
            if(j < K) {
                R[j] = S[i];
            }
        }

        for(int i = 0; i < K ; i++) {
            System.out.println(R[i]);
        }
    }
}
